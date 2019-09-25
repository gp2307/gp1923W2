package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class DWReleaseChick{

}
/**
  * DW 注册主题
  */
object DWReleaseChick {
  // 日志处理,获取当前类的日志
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)
  /**
    * 目标客户
    * 01
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String):Unit ={
    // 获取当前时间
    val begin = System.currentTimeMillis()
    try{
      // 导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._

      // 设置缓存级别
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite

      // 获取日志字段数据
      val customerColumns = DWReleaseColumnsHelper.selectDWReleaseCheckColumns()
      //设置条件 当天数据 获取目标用户：01
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        and
        col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CLICK.getCode))
      val customerReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,customerColumns)
        //填入条件
        .where(customerReleaseCondition)
        //重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)

      customerReleaseDF.show(10,false)
      //目标用户（存储）
      //      SparkHelper.writeTableDate(customerReleaseDF,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)
    }catch {
      //错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      //任务处理时长
      s"任务处理时长:${appName},bdp_day = ${bdp_day},${System.currentTimeMillis() - begin}"
    }
  }

  /**
    * 投放目标用户
    */
  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String)={
    val begin = System.currentTimeMillis()
    var spark:SparkSession = null
    try {
      //配置spark参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition","true")
        .set("hive.exec.dynamic.partition.mode","nonstrict")
        .set("spark.sql.shuffle.partitions","32")
        .set("hive.merge.mapfiles","true")
        .set("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold","50485760")
        .set("spark.sql.crossJoin.enabled","true" )
        .setMaster("local[*]")
        .setAppName(appName)
      //创建上下文
      spark = SparkHelper.createSpark(conf)
      //解析参数
      val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      //循环参数
      for(bdp_day <- timeRange){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex: Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      println(s"任务处理时长:,${System.currentTimeMillis() - begin}")
      spark.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "dw_release_job"
    val bdp_day_begin = "2019-09-24"
    val bdp_day_end = "2019-09-24"
    //执行job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }
}


package com.qf.bigdata.release.util

import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spark工具类
  */
object SparkHelper {
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)
  /**
    * 读取数据
    */
  def readTableData(spark: SparkSession,tableName:String,colNames:mutable.Seq[String]):DataFrame={
    import spark.implicits._
    val tableDF = spark.table(tableName)
      .selectExpr(colNames:_*)
    tableDF
  }

  /**
    * 写入数据
    */
  def writeTableDate(source:DataFrame,table:String,mode:SaveMode):Unit={
    //写入表
    source.write.mode(mode).insertInto(table)
  }
  def createSpark(conf:SparkConf):SparkSession={
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //加载自定义函数


    spark
  }

  /**
    * 参数校验
    */
  def rangeDates(begin:String,end:String):Seq[String]={
    val bdp_days = new ArrayBuffer[String]()
    try{
      val bdp_date_begin = DateUtil.dateFromat4String(begin)
      val bdp_date_end = DateUtil.dateFromat4String(end)
      //如果两个时间相同,取其中一个值
      //如果不相等，计算时间差
      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while (cday != bdp_date_end){
          bdp_days.+=(cday)
          //让初始时间累加，以天为单位
          cday =  DateUtil.dateFromat4StringDiff(cday,1)
        }
      }
    }catch {
      //错误信息处理
      case ex:Exception =>{
        println("参数不匹配")
        logger.error(ex.getMessage,ex)
      }
    }
    bdp_days
  }
}

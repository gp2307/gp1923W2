import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .set("hive.exec.dynamic.partition","true")
//      .set("hive.exec.dynamic.partition.mode","nonstrict")
//      .set("spark.sql.shuffle.partitions","32")
//      .set("hive.merge.mapfiles","true")
//      .set("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
//      .set("spark.sql.autoBroadcastJoinThreshold","50485760")
//      .set("spark.sql.crossJoin.enabled","true" )
//      .setMaster("local[*]")
//      .setAppName("test")
//    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//    spark.sql("select * from dw_release.dw_release_register_users limit 10").show()
//    spark.close()

    val list = List((1,2),(1,2))
    val map = Map((1,2),(1,2))
    println(map.get(1).get)
    println(map.getOrElse(1,0))
  }
}

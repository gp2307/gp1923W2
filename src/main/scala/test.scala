import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.warehouse","hdfs://hadoop001:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select count(*) from dm_release.dm_customer_cube").show()

    spark.close()
  }
}

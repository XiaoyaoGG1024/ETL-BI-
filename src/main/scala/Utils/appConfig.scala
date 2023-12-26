package Utils

import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 *
 * @author： 逍遥哥哥每天都要努力啊
 * @date： 2023/12/16
 * @description：
 * @modifiedBy：
 * @version: 1.0
 */
object appConfig {
  System.setProperty("HADOOP_USER_NAME", "root")
  def setupSparkSession(master: String, appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop01:9000/user/hive/warehouse") //db地址
      .config("hive.metastore.uris", "thrift://hadoop01:9083")  //元数据
      .config("spark.sql.parquet.writeLegacyFormat","true") //解决类型精度问题
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

}

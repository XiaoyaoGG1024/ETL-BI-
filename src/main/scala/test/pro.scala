package test

/**
 * Created with IntelliJ IDEA.
 *
 * @author： 逍遥哥哥每天都要努力啊
 * @date： 2023/6/18
 * @description：
 * @modifiedBy：
 * @version: 1.0
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object pro {


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("regiontopthree")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop01:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://hadoop01:9083")
      .enableHiveSupport()
      .getOrCreate()

    // 创建一个DataFrame，包含示例数据
    val data = Seq(
      ("四川", 1237370, 225569),
      ("内蒙古", 584508, 152501),
      ("浙江", 1155474, 212773),
      ("黑龙江", 466806, 205653),
      ("广东", 1273746, 304441),
      ("山东", 2401586, 1124122),
      ("甘肃", 533582, 148028),
      ("重庆", 428736, 169335),
      ("河北", 537672, 245273),
      ("湖北", 783024, 202751),
      ("贵州", 957990, 188709),
      ("福建", 540430, 100224),
      ("天津", 260016, 131933),
      ("陕西", 955768, 140333),
      ("山西", 446292, 173015),
      ("宁夏", 286458, 89566),
      ("广西", 797768, 209856),
      ("江西", 419730, 33353),
      ("吉林", 234282, 73142),
      ("新疆", 462364, 103519),
      ("安徽", 791516, 149903),
      ("青海", 109464, 19832),
      ("上海", 909512, 164802),
      ("辽宁", 391000, 193894),
      ("西藏", 96692, 9062),
      ("云南", 281852, 27278),
      ("江苏", 1234640, 370500),
      ("海南", 300936, 45088),
      ("河南", 1757634, 1002508),
      ("湖南", 612054, 283649),
      ("北京", 619286, 166901)
    )

    import spark.implicits._

    val df = data.toDF("province", "field1", "field2")
    // 定义自定义输出函数
    val outputFunction = udf((province: String) => s"${province}")
    // 添加文件名列
    val dfWithFileName = df.withColumn("省份", outputFunction($"province"))

    // 将结果按照省份字段分区，并写入不同的文件
    dfWithFileName.show(false)
//      .write
//      .mode(SaveMode.Overwrite)
//      .partitionBy("province")
//      .option("header", "true")
//      .csv("file:///D:/Scala/BigData/out")

  }
}

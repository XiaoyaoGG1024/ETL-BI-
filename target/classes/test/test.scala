package test
import Utils._


/**
 * @program: BigData
 * @description: ${description}
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/4/28
 * */
object test {
  def main(args: Array[String]): Unit = {

    val spark=appConfig.setupSparkSession("local[*]","test")

    spark.sql("show databases").show(false)

    spark.stop()
  }

}

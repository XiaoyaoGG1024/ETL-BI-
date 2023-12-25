package ODS

import Utils.appConfig
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

/**
 * @program: BigData
 * @description: 比赛先查看各个数据库表中是否有数据，如果有数据加一个where
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/13
 * */
object MysqlToHive {
  def main(args: Array[String]): Unit = {
    val spark=appConfig.setupSparkSession("local[*]","readMysql")

    //获取前一天的日期
    val dateStr = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    var dateFormat = dateStr.format(calendar.getTime)

    println("正在生成前一天日期")

    //mysql表
    val tableArray = Array("customer_inf", "order_detail", "order_master", "product_info")
    //封装JDBC
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")


    tableArray.foreach(x => {
      spark.read.jdbc("jdbc:mysql://192.168.202.134:3306/ds_db01", x, temp).createOrReplaceTempView(x)
      println(s"正在将mysql的${x}抽取到ods.${x}\n")
      spark.sql(
        s"""
           |insert overwrite table ods.${x} partition (elt_date=${dateFormat})
           |select * from ${x}
      """.stripMargin)
      println(s"抽取mysql的${x}到ods.${x}成功\n")
    })

    spark.close()
  }

}

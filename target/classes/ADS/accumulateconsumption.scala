package ADS

import Utils.appConfig
import java.util.Properties
/**
 * Created with IntelliJ IDEA.
 *
 * @author： 逍遥哥哥每天都要努力啊
 * @date： 2023/12/16
 * @description：
 * @modifiedBy：
 * @version: 1.0
 */
object accumulateconsumption {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "accumulateconsumption")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
    //计算2022年4月26日凌晨0点0分0秒到早上9点59分59秒为止，该时间段每小时的新增订单金额与当天订单总金额累加值 折线
//    +-------------+--------------+--------------+
//    | create_time | consumptionadd | consumptionacc |
//    +-------------+--------------+--------------+
//    |
//    2022 - 04 - 2603 | 34010.22 | 34010.22 |
//      |
//    2022 - 04 - 2604 | 4838.12 | 38848.34 |
//      |
//    2022 - 04 - 2605 | 206288.44 | 245136.78 |
//      |
//    2022 - 04 - 2606 | 395444.56 | 640581.34 |
//      |
//    2022 - 04 - 2607 | 406811.89 | 1047393.23 |
//      |
//    2022 - 04 - 2608 | 424903.59 | 1472296.82 |
//      |
//    2022 - 04 - 2609 | 444881.79 | 1917178.61 |
//      +-------------+--------------+--------------+

    spark.sql(
      """
        |select
        |create_time,
        |sum(order_money) as consumptionadd,
        |sum(order_money) OVER (ORDER BY create_time) AS consumptionacc
        |from(
        |SELECT
        |             date_format(create_time,"yyyy-MM-dd HH") as create_time,
        |             sum(order_money) as order_money
        |FROM
        |     dwd.fact_order_master
        |WHERE
        |       create_time >= '2022-04-26 00:00:00' AND create_time <= '2022-04-26 09:59:59' and  order_status = '已下单'
        |GROUP BY
        |      date_format(create_time,"yyyy-MM-dd HH")
        |    ) a
        |group by  create_time, order_money
      """.stripMargin).write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "accumulateconsumption", temp)


    spark.stop()
  }

}

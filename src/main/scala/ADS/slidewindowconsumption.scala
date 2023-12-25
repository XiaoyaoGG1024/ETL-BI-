package ADS

import Utils.appConfig

/**
 * Created with IntelliJ IDEA.
 *
 * @author： 逍遥哥哥每天都要努力啊
 * @date： 2023/12/16
 * @description：
 * @modifiedBy：
 * @version: 1.0
 */
object slidewindowconsumption {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "slidewindowconsumption")
    //计算2022年4月26日凌晨0点0分0秒到早上9点59分59秒为止的数据，以5个小时为时间窗口，滑动的步长为1小时，
    // 做滑动窗口计算该窗口内订单总金额和订单总量，时间不满5小时不触发计算（即从凌晨5点0分0秒开始触发计算）
    spark.sql(
      """
        |select distinct
        |     date_format(create_time,"yyyy-MM-dd HH") as create_time,
        |     total_amount,
        |     total_count,
        |     round(total_amount/total_count,2) as consumptionavg from(
        |SELECT
        |  create_time,
        |  SUM(order_money) AS total_amount,
        |  COUNT(*) AS total_count
        |FROM
        |  dwd.fact_order_master
        |WHERE
        |  create_time >='2022-04-26 00:00:00' AND create_time <= '2022-04-26 09:59:59'
        |GROUP BY
        |  create_time
        |HAVING
        |  HOUR(create_time) >= 5)
        |  group by date_format(create_time,"yyyy-MM-dd HH"),total_amount,total_count
      """.stripMargin).show(false)


    spark.stop()
  }

}

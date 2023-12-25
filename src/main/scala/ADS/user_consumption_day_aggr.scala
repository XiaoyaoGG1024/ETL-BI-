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
object user_consumption_day_aggr {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "slidewindowconsumption")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
//        CREATE TABLE `user_consumption_day_aggr`(
//          `customer_id` int,
//          `customer_name` string,
//          `total_amount` double,
//          `total_count` int,
//          `year` int,
//          `month` int,kk
//          `day` int)
    //每人每天下单的数量和下单的总金额前十  折线
    //    +-----------+-------------+------------+-----------+----------+
    //    | customer_id | customer_name | total_amount | total_count | date |
    //    +-----------+-------------+------------+-----------+----------+
    //    |
    //    1 | 郝桂花 | 25258.96 | 4 | 2022 - 04 - 03 |
    //      |
    //    1 | 郝桂花 | 17883.96 | 4 | 2022 - 04 - 02 |
    //      |
    //    1 | 郝桂花 | 10208.72 | 4 | 2022 - 03 - 31 |
    //      |
    //    1 | 郝桂花 | 8708.64 | 4 | 2022 - 05 - 05 |
    //      |
    //    1 | 郝桂花 | 4973.70 | 5 | 2022 - 05 - 02 |
    spark.sql(
      """
        |select
        |     distinct
        |     a.customer_id,
        |     a.customer_name,
        |     sum(b.payment_money) as total_amount,
        |     count(b.order_id) as total_count,
        |     from_unixtime(unix_timestamp(b.elt_date, 'yyyyMMdd'), 'yyyy-MM-dd')as date
        |from
        |     dwd.dim_customer_inf a,
        |     dwd.fact_order_master b
        |where
        |     a.customer_id=b.customer_id
        |group by
        |     a.customer_id,
        |     a.customer_name,
        |     date
        |order by
        |     total_amount
        |desc
        |limit 10
  """.stripMargin)
      .write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "user_consumption_day_aggr",
        temp)

    



    spark.stop()

  }

}

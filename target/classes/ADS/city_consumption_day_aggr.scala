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
object city_consumption_day_aggr {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "city_consumption_day_aggr")

    //        CREATE TABLE `city_consumption_day_aggr`(
    //          `city_name` string,
    //          `province_name` string,
    //          `total_amount` double,
    //          `total_count` int,
    //          `year` int,
    //          `month` int,
    //          `sequencefile` int
    //          )

    //每个城市每月下单的数量和下单的总金额
    //    +------------+--------+------------+-----------+----+-----+--------+
    //    |city        |province|total_amount|total_count|year|month|sequence|
    //    +------------+--------+------------+-----------+----+-----+--------+
    //    |浙江省嘉兴市|浙江省  |20530       |4          |2022|9    |10      |
    //      |江苏省淮安市|江苏省  |19327       |4          |2022|9    |14      |
    //      |江苏省姜堰市|江苏省  |12011       |4          |2022|9    |16      |
    //      |浙江省慈溪市|浙江省  |3522        |4          |2022|8    |13      |
    //      |江苏省海门市|江苏省  |6762        |5          |2022|9    |18      |
    //      +------------+--------+------------+-----------+----+-----+--------+
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
    spark.sql(
      """
        |select
        |       *,
        |       row_number() over(partition by province,year,month order by total_amount desc ) sequence
        |from
        |(select
        |      city,
        |      province,
        |      sum(payment_money) as total_amount,
        |      count(order_id) as total_count,
        |      year(from_unixtime(unix_timestamp(elt_date, 'yyyyMMdd'), 'yyyy-MM-dd'))as year,
        |      month(from_unixtime(unix_timestamp(elt_date, 'yyyyMMdd'), 'yyyy-MM-dd'))as month
        |from
        |    dwd.fact_order_master
        |group by
        |      city,
        |      province,
        |      year(from_unixtime(unix_timestamp(elt_date, 'yyyyMMdd'), 'yyyy-MM-dd')),
        |      month(from_unixtime(unix_timestamp(elt_date, 'yyyyMMdd'), 'yyyy-MM-dd'))
        |)
      """.stripMargin).createOrReplaceTempView("data")
//    spark.sql("select * from data").show(false)
    spark.sql(
      """
        |select
        |       city,
        |       province,
        |       cast(total_amount as bigint),
        |       total_count,
        |       year,
        |       month,
        |       sequence
        |from
        |     data
        |order by
        |   total_count,
        |   total_amount
        |desc
        |limit 5
      """.stripMargin).write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "city_consumption_day_aggr", temp)


    spark.stop()

  }

}

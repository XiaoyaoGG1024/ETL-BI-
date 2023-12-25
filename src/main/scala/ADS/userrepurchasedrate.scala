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
object userrepurchasedrate {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "userrepurchasedrate")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
//    请计算连续两天下单的用户与已下单用户的占比
//    +-------------+---------------+--------------+
//    | purchaseduser | repurchaseduser | repurchaserate |
//    +-------------+---------------+--------------+
//    |
//    19871 | 3139 | 20.0 % |
//    +-------------+---------------+--------------+
    spark.sql(
      """
      select count(*) as repurchaseduser
      from (select distinct shipping_user
              from (select *,
                    datediff(new_data, create_time) as diff
                              from (SELECT order_sn,shipping_user,create_time,
                                        lead(create_time, 1) over ( partition by shipping_user order by create_time) as new_data
                                    FROM (select * from dwd.fact_order_master where order_status = '已下单') as d) a) b
            where diff = 1)
""".stripMargin).createOrReplaceTempView("table1")

    spark.sql("select count(distinct shipping_user)as purchaseduser from dwd.fact_order_master where order_status = '已下单'").createOrReplaceTempView("table2")
    spark.sql("select * from table1").show(false)
    spark.sql("select * from table2").show(false)
    spark.sql(
      """ select
        |purchaseduser,repurchaseduser,concat(round((repurchaseduser/purchaseduser),1)*100,"%")as repurchaserate
        |from table1 cross join table2""".stripMargin).show(false)
//      .write
//      .mode("overwrite")
//      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false", "userrepurchasedrate", temp)



    spark.stop()
  }

}

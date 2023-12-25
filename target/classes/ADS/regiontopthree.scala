package ADS

import Utils.appConfig

import java.util.Properties

/**
 * @program: BigData
 * @description: ${description}
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/13
 * */
object regiontopthree {
  def main(args: Array[String]): Unit = {
    val spark=appConfig.setupSparkSession("local[*]","regiontopthree")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")

    //    CREATE TABLE citymidcmpprovince
    //    (`provincename` TEXT,
    //      `citynames` TEXT,
    //      `citymount` TEXT
    //
    //    )
    //    ENGINE = MergeTree()
    //    ORDER BY provincename

    spark.sql(
      """
        |
        |    SELECT
        |        province,
        |        city,
        |        SUM(order_money) AS order_amount
        |    FROM
        |        dwd.fact_order_master
        |    WHERE
        |        YEAR(create_time) = 2022
        |    GROUP BY
        |        province,
        |        city
        |    order by
        |         order_amount
    """.stripMargin)
      .write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false","regiontopthree",temp)
//每个省份2022年订单金额前3省份
//    spark.sql(
//      """
//        |SELECT
//        |    province AS provincename,
//        |    CONCAT_WS(',', COLLECT_LIST(city)[0], COLLECT_LIST(city)[1], COLLECT_LIST(city)[2]) AS citynames,
//        |    CONCAT_WS(',', ROUND(COLLECT_LIST(order_amount)[0]), ROUND(COLLECT_LIST(order_amount)[1]), ROUND(COLLECT_LIST(order_amount)[2])) AS cityamount
//        |FROM
//        |    (
//        |    SELECT
//        |        province,
//        |        city,
//        |        SUM(order_money) AS order_amount
//        |    FROM
//        |        dwd.fact_order_master
//        |    WHERE
//        |        YEAR(create_time) = 2022
//        |    GROUP BY
//        |        province,
//        |        city
//        |    ) t
//        |GROUP BY
//        |    province
//        |ORDER BY
//        |    provincename ASC
//        |LIMIT 3
//    """.stripMargin).show(false)


    spark.stop()

  }

}

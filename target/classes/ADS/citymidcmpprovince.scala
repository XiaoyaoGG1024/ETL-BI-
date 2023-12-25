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
object citymidcmpprovince {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "citymidcmpprovince")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
    import spark.implicits._

//        CREATE TABLE citymidcmpprovince
//        (
//          `cityname` TEXT,
//          `citymidconsumption` DOUBLE,
//          `provincename` TEXT,
//          `provincemidconsumption` DOUBLE,
//          `comparison` TEXT
//        )
    //    ENGINE = MergeTree()
    //    ORDER BY provinceavgconsumption

    //每个城市每个月平均订单金额和该城市所在省份订单金额中位数
//    +------------+-----------+------------+---------------+----------+
//    | cityname | city_median | provincename | province_median | comparison |
//    +------------+-----------+------------+---------------+----------+
//    | 浙江省嘉兴市 |
//    3841.400 | 浙江省 | 3611.290 | 高 |
//      | 浙江省舟山市 |
//    3625.005 | 浙江省 | 3611.290 | 高 |
//      | 浙江省海宁市 |
//    3789.420 | 浙江省 | 3611.290 | 高 |
//      | 浙江省温州市 |
//    3691.545 | 浙江省 | 3611.290 | 高 |
//      | 浙江省上虞市 |
//    3705.000 | 浙江省 | 3611.290 | 高 |
//      | 浙江省宁波市 |
//    3546.120 | 浙江省 | 3611.290 | 低 |
//      | 浙江省义乌市 |
//    3585.250 | 浙江省 | 3611.290 | 低 |
//      | 浙江省金华市 |
//    3378.650 | 浙江省 | 3611.290 | 低 |
    spark.sql(
      """|select city     as cityname,
         |       city_median,
         |       t1.province as provincename,
         |       province_median,
         |       case
         |           when city_median > province_median then '高'
         |           when city_median < province_median then '低'
         |           else '相等'
         |           end  as comparison
         |from (
         |         (SELECT distinct city,
         |                 province,
         |                 case
         |                     when abs(cast(r1 as int) - cast(r2 as int)) = 1 then 0.5 * sum(order_money) over (partition by city)
         |                     when cast(r1 as int) = cast(r2 as int) then order_money end as city_median
         |          from (select city,
         |                       province,
         |                       order_money,
         |                       row_number() over (partition by city ORDER BY order_money desc, order_id desc) r1,
         |                       row_number() over (partition by city order by order_money, order_id)      r2
         |                from dwd.fact_order_master) t
         |          where abs(cast(r1 as int) - cast(r2 as int)) = 1
         |                                      or cast(r1 as int) = cast(r2 as int)) as t1 join (SELECT distinct province,
         |                                            case
         |                                                when abs(cast(r1 as int) - cast(r2 as int)) = 1
         |                                                    then 0.5 * sum(order_money) over (partition by province)
         |                                                when cast(r1 as int) = cast(r2 as int) then order_money end as province_median
         |                                     from (select province,
         |                                                  order_money,
         |                                                  row_number() over (partition by province ORDER BY order_money desc, order_id desc )  r1,
         |                                                row_number() over (partition by province order by order_money, order_id)      r2
         |                                           from dwd.fact_order_master) t
         |                                     where abs(cast(r1 as int) - cast(r2 as int)) = 1
         |                                        or cast(r1 as int) = cast(r2 as int)) t2 on t1.province = t2.province)
      """.stripMargin).write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "citymidcmpprovince", temp)


    spark.stop()

  }

}

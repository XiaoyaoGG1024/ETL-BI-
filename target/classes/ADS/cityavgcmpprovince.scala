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
object cityavgcmpprovince {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "cityavgcmpprovince")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
//        CREATE TABLE cityavgcmpprovince
//        (
//          `cityname` TEXT,
//          `cityavgconsumption` DOUBLE,
//          `provincename` TEXT,
//          `provinceavgconsumption` DOUBLE,
//          `comparison` TEXT
//        )
    //    ENGINE = MergeTree()
    //    ORDER BY provinceavgconsumption

    //每个城市每个月平均订单金额和该城市所在省份平均订单金额相比较结果（“高/低/相同”） 柱状图
//    +------------+-----------+--------+-----------+----------+
//    | city | cityAvg | province | provinceAvg | comparison |
//    +------------+-----------+--------+-----------+----------+
//    | 浙江省嘉兴市 |
//    4473.172022 | 浙江省 | 4238.095623 | 高 |
//      | 浙江省舟山市 |
//    4286.273653 | 浙江省 | 4238.095623 | 高 |
//      | 浙江省海宁市 |
//    4409.033654 | 浙江省 | 4238.095623 | 高 |
//      | 浙江省温州市 |
//    4223.591332 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省上虞市 |
//    4138.365446 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省宁波市 |
//    4198.327198 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省义乌市 |
//    4233.265649 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省金华市 |
//    4072.235598 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省奉化市 |
//    4057.307243 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省台州市 |
//    4065.794808 | 浙江省 | 4238.095623 | 低 |
//      | 浙江省慈溪市 |
//    4408.924931 | 浙江省 | 4238.095623 | 高 |
//      | 浙江省杭州市 |
//    4270.102058 | 浙江省 | 4238.095623 | 高 |
//      | 浙江省绍兴市 |
//    4177.138290 | 浙江省 | 4238.095623 | 低 |
//      | 贵州省贵阳市 |
//    4361.051291 | 贵州省 | 4361.051291 | 相等 |
//      | 广东省河源市 |
//    4474.412597 | 广东省 | 4474.412597 | 相等 |
//      | 上海市 |
//    4270.576729 | 上海市 | 4270.576729 | 相等 |
    spark.sql(
      """
        |select
        |       city,
        |       cityAvg,
        |       province,
        |       provinceAvg,
        |       case  when cityAvg > provinceAvg then "高"
        |             when cityAvg < provinceAvg then "低"
        |             else "相等"
        |             end comparison
        |from
        |(select distinct
        |       city,
        |       province,
        |       avg(order_money) over(partition by city) as cityAvg,
        |       avg(order_money) over(partition by province) as provinceAvg
        |from
        |     dwd.fact_order_master)
  """.stripMargin).write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "cityavgcmpprovince", temp)


    spark.stop()
  }

}

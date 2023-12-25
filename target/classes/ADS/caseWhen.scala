package ADS

import Utils.appConfig
import org.apache.spark.sql.Row
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
object caseWhen {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "caseWhen")
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")
    import spark.implicits._

//    +-------------+------+
//    | province_name | Amount |
//    +-------------+------+
//    | 上海市 |
//    193957 |
//      | 江苏省 |
//    78432 |
//      | 浙江省 |
//    53229 |
//      | 贵州省 |
//    4345 |
//      | 广东省 |
//    1136 |
//      +-------------+------+ val df =
   spark.sql(
      """select
              province as province_name,
              count(order_id) as Amount
          from
              dwd.fact_order_master
          group by province
          order by count(order_id) Desc""".stripMargin).write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "caseWhen", temp)

//    df.show(false)
//    df.createOrReplaceTempView("data")
//
//    val rows: Array[Row] = df.collect()
//    var sql = "select \n"
//    var a = 0
//    for (a <- 0 to rows.length - 1) {
//      if (a == rows.length - 1)
//        sql = sql + s"max(case when province_name='${rows(a)(0)}' then Amount end )  as `${rows(a)(0)}`" + "\n"
//      else
//        sql = sql + s"max(case when province_name='${rows(a)(0)}' then Amount end )  as `${rows(a)(0)}`" + ",\n"
//    }
//    sql = sql + "from data"
//    spark.sql(sql).show(false)

    spark.stop()
  }

}

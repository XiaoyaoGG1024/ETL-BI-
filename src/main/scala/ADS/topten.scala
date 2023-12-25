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
object topten {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "topten")

    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")

//        CREATE TABLE topten
//        (`topquantityid` INT,
//          `topquantityname` TEXT,
//          `topquantity` INT,
//          'toppriceid' TEXT,
//          'toppricename' TEXT,
//          'topprice'  DECIMAL(8,2),
//          'sequence' INT
//        )
    //    ENGINE = MergeTree()
    //    ORDER BY sequence

    //，计算销售量前10的商品

    spark.sql(
      """
        |SELECT
        |	*
        |FROM
        |	(
        |SELECT
        |	*,
        |	row_number ( ) over ( ORDER BY topprice DESC ) AS sequence
        |FROM
        |	(
        |SELECT
        |	a.product_id AS topquantityid,
        |	a.product_name AS topquantityname,
        |	count( a.order_detail_id ) AS topquantity,
        |	b.product_id AS toppriceid,
        |	b.product_name AS toppricename,
        |	sum( b.price ) AS topprice
        |FROM
        |	dwd.fact_order_detail AS a,
        |	dwd.dim_product_info AS b
        |WHERE
        |	a.product_id = b.product_id
        |GROUP BY
        |	a.product_id,
        |	a.product_name,
        |	b.product_id,
        |	b.product_name
        |	) t1
        |	) t2
        |WHERE
        |	sequence <= 10
      """.stripMargin).write
      .mode("overwrite")
      .jdbc("jdbc:mysql://192.168.202.134:3306/ads?ue&chuseUnicode=traracterEncoding=utf8&useSSL=false",
        "topten",
        temp)

    spark.stop()


  }

}

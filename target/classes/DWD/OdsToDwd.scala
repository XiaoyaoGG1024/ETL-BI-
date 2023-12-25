import Utils.appConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

/**
 * @program: BigData
 * @description: 比赛先查看各个数据库表中是否有数据，如果有数据加一个where
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/13
 */
object OdsToDwd{
        def main(args:Array[String]):Unit={

        val spark=appConfig.setupSparkSession("local[*]","qingxishuju")


        //获取前一天的日期
        val dateStr1=new SimpleDateFormat("yyyyMMdd")
        val calendar1=Calendar.getInstance()
        calendar1.add(Calendar.DATE,-1)
        var dateFormat1=dateStr1.format(calendar1.getTime)
        println(dateFormat1)

        //获取当前
        val dateStr=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val calendar=Calendar.getInstance()
        var dateFormat=dateStr.format(calendar.getTime)
        println(dateFormat)

        //动态分区
        //开启动态分区，非严格模式
        spark.sql(
        """
        |set hive.exec.dynamic.partition=true
        |""".stripMargin)
        spark.sql(
        """
        |set hive.exec.dynamic.partition.mode=nonstrict
        |""".stripMargin)


        val tableArray=Array("dim_customer_inf","fact_order_detail","fact_order_master","dim_product_info")

        /**
         * dim_customer_inf
         */
        println("--------------------正在清洗dim_customer_inf-----------------------------")
        spark.sql(s"select * from ods.customer_inf where elt_date=${dateFormat1}")
        .withColumn("dwd_insert_user",lit("user1"))
        .withColumn("dwd_insert_time",lit(lit(dateFormat)))
        .withColumn("dwd_modify_user",lit("user1"))
        .withColumn("dwd_modify_time",lit(lit(dateFormat)))
        .createOrReplaceTempView("ods")

        //    spark.sql("select * from ods").show(false)

        spark.sql(
        s"""
         |insert overwrite table dwd.dim_customer_inf partition (elt_date=${dateFormat1})
         |select
         |customer_inf_id,
         |customer_id,
         |customer_name,
         |identity_card_type,
         |identity_card_no,
         |mobile_phone,
         |customer_email,
         |gender,
         |customer_point,
         |register_time,
         |birthday,
         |customer_level,
         |customer_money,
         |modified_time,
         |dwd_insert_user,
         |dwd_insert_time,
         |dwd_modify_user,
         |dwd_modify_time
         |from
         |    ods
          """.stripMargin)
        println("--------------------清洗dim_customer_inf成功-----------------------------")

        /**
         * product_info
         */
        println("--------------------正在清洗product_info-----------------------------")
        spark.sql(s"select * from ods.product_info where elt_date=${dateFormat1}")
        .withColumn("dwd_insert_user",lit("user1"))
        .withColumn("dwd_insert_time",lit(lit(dateFormat)))
        .withColumn("dwd_modify_user",lit("user1"))
        .withColumn("dwd_modify_time",lit(lit(dateFormat)))
        .createOrReplaceTempView("ods1")

        //    spark.sql("select * from ods1").show(false)
        spark.sql(
        s"""
         |insert overwrite  table dwd.dim_product_info partition(elt_date=${dateFormat1})
         |select
         |       product_id,
         |       product_core,
         |       product_name,
         |       bar_code,
         |       brand_id,
         |       one_category_id,
         |       two_category_id,
         |       three_category_id,
         |       supplier_id,
         |       price,
         |       average_cost,
         |       publish_status,
         |       audit_status,
         |       weight,
         |       length,
         |       height,
         |       width,
         |       color_type,
         |       production_date,
         |       shelf_life,
         |       descript,
         |       indate,
         |       modified_time,
         |       dwd_insert_user,
         |       dwd_insert_time,
         |       dwd_modify_user,
         |       dwd_modify_time
         |from
         |     ods1
      """.stripMargin)
        println("--------------------清洗product_info成功-----------------------------")

        /**
         * fact_order_master
         */
        println("--------------------正在清洗fact_order_master-----------------------------")
        spark.sql(s"select * from ods.order_master where elt_date=${dateFormat1}")
        .withColumn("dwd_insert_user",lit("user1"))
        .withColumn("dwd_insert_time",lit(lit(dateFormat)))
        .withColumn("dwd_modify_user",lit("user1"))
        .withColumn("dwd_modify_time",lit(lit(dateFormat)))
        .createOrReplaceTempView("ods3")

        spark.sql(
        """
        |insert  overwrite  table dwd.fact_order_master partition(elt_date)
        |select
        |     order_id,
        |     order_sn,
        |     customer_id,
        |     shipping_user,
        |     province,
        |     city,
        |     address,
        |     order_source,
        |     payment_method,
        |     order_money,
        |     district_money,
        |     shipping_money,
        |     payment_money,
        |     shipping_comp_name,
        |     shipping_sn,
        |     from_unixtime(unix_timestamp(create_time, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss'),
        |     from_unixtime(unix_timestamp(shipping_time, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss'),
        |     from_unixtime(unix_timestamp(pay_time, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss'),
        |     from_unixtime(unix_timestamp(receive_time, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss'),
        |     order_status,
        |     order_point,
        |     invoice_title,
        |     modified_time,
        |     dwd_insert_user,
        |     dwd_insert_time,
        |     dwd_modify_user,
        |     dwd_modify_time,
        |     from_unixtime(unix_timestamp(create_time,'yyyyMMddHHmmss'),'yyyyMMdd') as elt_date
        |from
        |     ods3
        |where
        |     where  length(city)<8
      """.stripMargin)
        println("--------------------清洗fact_order_master成功-----------------------------")

        /**
         * fact_order_detail
         */
        println("--------------------正在清洗fact_order_detail-----------------------------")
        spark.sql(s"select * from ods.order_detail where elt_date=${dateFormat1}")
        .withColumn("dwd_insert_user",lit("user1"))
        .withColumn("dwd_insert_time",lit(lit(dateFormat)))
        .withColumn("dwd_modify_user",lit("user1"))
        .withColumn("dwd_modify_time",lit(lit(dateFormat)))
        .createOrReplaceTempView("ods4")


        spark.sql(
        """
        |insert  overwrite  table dwd.fact_order_detail partition(elt_date)
        |select
        |      order_detail_id,
        |      order_sn,
        |      product_id,
        |      product_name,
        |      product_cnt,
        |      product_price,
        |      average_cost,
        |      weight,
        |      fee_money,
        |      w_id,
        |      from_unixtime(unix_timestamp(create_time, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss'),
        |      modified_time,
        |      dwd_insert_user,
        |      dwd_insert_time,
        |      dwd_modify_user,
        |      dwd_modify_time,
        |      from_unixtime(unix_timestamp(create_time,'yyyyMMddHHmmss'),'yyyyMMdd') as elt_date
        |from
        |     ods4
      """.stripMargin)
        println("--------------------清洗fact_order_detail成功-----------------------------")


        spark.stop()

        }

        }

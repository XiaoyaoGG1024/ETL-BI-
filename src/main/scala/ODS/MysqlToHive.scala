package ODS

import Utils.appConfig
import Utils.tableType.mapToHiveDataType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

/**
 * @program: BigData
 * @description: 比赛先查看各个数据库表中是否有数据，如果有数据加一个where
 * drop database if exists ods cascade
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/13
 * */
object MysqlToHive {
  def main(args: Array[String]): Unit = {
    val spark = appConfig.setupSparkSession("local[*]", "readMysql")

    //获取前一天的日期
    val dateStr = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    var dateFormat = dateStr.format(calendar.getTime)
    println("正在生成前一天日期")

    //封装JDBC
    val temp = new Properties()
    temp.put("user", "root")
    temp.put("password", "123456")
    temp.put("driver", "com.mysql.jdbc.Driver")

    val jdbcUrl = "jdbc:mysql://192.168.202.134:3306/ds_db01"

    // 执行SQL查询以获取所有表名
    val tableNameDF = spark.read.jdbc(jdbcUrl, "information_schema.tables", temp)
      .select("TABLE_NAME")
      .filter("TABLE_SCHEMA = 'ds_db01'")
    // 将结果以数组形式收集
    val tableNames = tableNameDF.collect().map(row => row.getString(0))

    // 打印表名
    //    tableNames.foreach(println)
    //隐式转换
    val hiveDataBase="ods"
    val partitionBycolNames = "elt_date=" + dateFormat
    //新增列
    val newColumn = StructField("elt_date", StringType, nullable = true)
    //元数据更新
    spark.sql("set hive.msck.path.validation=ignore")
    tableNames.foreach(x => {
      //      spark.read.jdbc(jdbcUrl, x, temp).createOrReplaceTempView(x)
      //      spark.sql(
      //        s"""
      //           |insert overwrite table ods.${x} partition (elt_date=${dateFormat})
      //           |select * from ${x}
      //      """.stripMargin)
      //  part-00000-6459747d-ca33-4c59-b2ac-fff8004059cb-c000

      var mysqlDF = spark.read.jdbc(jdbcUrl, x, temp)
      //mysql表结构转换
      val hiveTableSchema = (newColumn +: mysqlDF.schema).map { field =>
        val hiveDataType = mapToHiveDataType(field.dataType.typeName)
        field.copy(dataType = hiveDataType)
      }
      val hiveTableName = x
      val partitionColumn = "elt_date"
      val hiveTableExists = spark.sql(s"SHOW TABLES LIKE '$hiveTableName'").count() > 0

      if (!hiveTableExists) {
        //sql拼接HIve建表
        val columnsDDL = hiveTableSchema.map(field => s"${field.name} ${field.dataType.sql}").mkString(", ")
        val createTableDDL = s"CREATE TABLE IF NOT EXISTS ${hiveDataBase}.$hiveTableName ($columnsDDL) " +
          s"USING PARQUET PARTITIONED BY ($partitionColumn)"
        //通过sparksql会自动刷新元数据
        spark.sql(createTableDDL)
      } else {
        println(s"The Hive table $hiveTableName already exists. Skipping table creation.")
      }
      //全量抽取
      mysqlDF.write.mode(SaveMode.Overwrite)
        .format("parquet")
        .save(s"/user/hive/warehouse/${hiveDataBase}.db/${x}/${partitionBycolNames}")
      //	part-00000-d48b2691-182c-445f-a2cb-62b7720ec173-c000.snappy.parquet
      //元数据更新
      spark.sql(s"msck repair table ${hiveDataBase}.${hiveTableName}")
      println(s"抽取mysql的${x}到${hiveDataBase}.${x}成功")
    })

    spark.close()
  }

}

package Utils

/**
 * Created with IntelliJ IDEA.
 *
 * @author： 逍遥哥哥每天都要努力啊
 * @date： 2023/12/26
 * @description：
 * @modifiedBy：
 * @version: 1.0
 */
object tableType {
  def mapToHiveDataType(mysqlDataType: String): org.apache.spark.sql.types.DataType = {
    mysqlDataType.toLowerCase match {
      case "int" => org.apache.spark.sql.types.IntegerType
      case "bigint" => org.apache.spark.sql.types.LongType
      case "varchar" | "char" => org.apache.spark.sql.types.StringType
      case "double" => org.apache.spark.sql.types.StringType
      case _ => org.apache.spark.sql.types.StringType
    }
  }

}

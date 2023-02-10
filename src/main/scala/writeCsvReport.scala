package aliexpress
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class AliexpressReportWriter{
  def writeCsvReport(spark: SparkSession,aliexpressDF:DataFrame,fileName:String) {
        aliexpressDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv(fileName)
  }

}
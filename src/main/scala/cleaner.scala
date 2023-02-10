package aliexpress
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

  class AliexpressCleaner {
    def cleanAliexpressDataset(spark: SparkSession,aliexpressDF:DataFrame): DataFrame = {
      val cleanedDF = aliexpressDF.drop("category")  // one value in whole column
      cleanedDF

    }

}
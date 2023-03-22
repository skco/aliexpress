import org.apache.spark.sql.{ DataFrame, Dataset, Row,  SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class AliexpressLoader{

  /**
   *
   * @param spark spark session
   * @param datasetFilePath raw aliexpress csv dataset
   * @return formated DataFrae
   */

    def loadAliexpressDataset(spark: SparkSession,datasetFilePath:String):Dataset[Row] = {
      //custom schema for datetime parse
      val customSchema = StructType(Array(
        StructField("id",             DataTypes.LongType, nullable = true),
        StructField("storeId",        DataTypes.LongType, nullable = true),
        StructField("storeName",      DataTypes.StringType, nullable = true),
        StructField("title",          DataTypes.StringType, nullable = true),
        StructField("rating",         DataTypes.DoubleType, nullable = true),
        StructField("lunchTime",      DataTypes.TimestampType, nullable = true),
        StructField("category",       DataTypes.StringType, nullable = true),
        StructField("postCategory",   DataTypes.IntegerType, nullable = true),
        StructField("sold",           DataTypes.StringType, nullable = true),
        StructField("price",          DataTypes.DoubleType, nullable = true),
        StructField("discount",       DataTypes.DoubleType, nullable = true),
        StructField("shippingCost",   DataTypes.DoubleType, nullable = true),
        StructField("imageUrl",       DataTypes.StringType, nullable = true),
        StructField("storeUrl",       DataTypes.StringType, nullable = true),
        StructField("category_name",  DataTypes.StringType, nullable = true),
        StructField("category_id",    DataTypes.StringType, nullable = true),
        StructField("type",           DataTypes.StringType, nullable = true)))

      //dataset source
      //https://www.kaggle.com/datasets/abdullahbuzaid/ali-express-data

      //prices in SAR
      //1SAR = 0.27USD
      //12-14h of December 2022

      spark.read
        .option("header", "true")
        .schema(customSchema)
        .csv(datasetFilePath)
        .withColumn("sold", regexp_replace(col("sold"), " sold", "")) // remove " sold" from string to get pcs as integer
        .withColumn("sold", col("sold").cast("Integer"))
        .withColumn("price", col("price") * 0.27) //1 SAR = 0.27 USD
        .withColumn("shippingCost", col("shippingCost") * 0.27) //1 SAR = 0.27 USD
        .withColumn("shippingCost", round(col("shippingCost"), 2)) //round double columns after multiplication
        .withColumn("price", round(col("price"), 2))
    }
}
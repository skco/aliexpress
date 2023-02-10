package aliexpress
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class AliexpressLoader {
    def loadAliexpressDataset(spark: SparkSession,datasetFilePath:String):DataFrame = {
      //custom scema for datetime parse
      val customSchema = StructType(Array(
        StructField("id",             DataTypes.LongType, true),
        StructField("storeId",        DataTypes.LongType, true),
        StructField("storeName",      DataTypes.StringType, true),
        StructField("title",          DataTypes.StringType, true),
        StructField("rating",         DataTypes.DoubleType, true),
        StructField("lunchTime",      DataTypes.TimestampType, true),
        StructField("category",       DataTypes.StringType, true),
        StructField("postCategory",   DataTypes.IntegerType, true),
        StructField("sold",           DataTypes.StringType, true), // remove " sold"
        StructField("price",          DataTypes.DoubleType, true),
        StructField("discount",       DataTypes.DoubleType, true),
        StructField("shippingCost",   DataTypes.DoubleType, true),
        StructField("imageUrl",       DataTypes.StringType, true),
        StructField("storeUrl",       DataTypes.StringType, true),
        StructField("category_name",  DataTypes.StringType, true),
        StructField("category_id",    DataTypes.StringType, true),
        StructField("type",           DataTypes.StringType, true)))

      //dataset source
      //https://www.kaggle.com/datasets/abdullahbuzaid/ali-express-data

      //prices in SAR
      //1SAR = 0.27USD
      //12-14h of December 2022

      val aliexpressDF:Dataset[Row] = spark.read
        .option("header", "true")
        .schema(customSchema)
        .csv(datasetFilePath)
        .withColumn("sold", regexp_replace(col("sold"), " sold", "")) // remove " sold" from string to get pcs as integer
        .withColumn("sold", col("sold").cast("Integer"))
        .withColumn("price", col("price") * 0.27) //1 SAR = 0.27 USD
        .withColumn("shippingCost", col("shippingCost") * 0.27) //1 SAR = 0.27 USD
        .withColumn("shippingCost", round(col("shippingCost"), 2)) //round double columns after multiplication
        .withColumn("price", round(col("price"), 2))
        aliexpressDF
    }
}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

  class AliexpressAnalyzer {
    /**
     * c
     * @param aliexpressDF cleaned DataFrame
     * @return DataFrame with counted item in each category
     */
    def countItemsInCategories(aliexpressDF:Dataset[Row] ):Dataset[Row]  = {
      val itemInCategories: DataFrame = aliexpressDF
        .groupBy("category_name")
        .agg(count("category_name")
        .alias("count"))
        .sort(desc("count"))
      itemInCategories
    }

    /**
     *
     * @param aliexpressDF leaned DataFrame
     * @return ataFrame with total sold value in each category
     */
    def calculateTotalSoldValueByCategory(aliexpressDF: Dataset[Row] ): Dataset[Row]  = {
      val withSoldValue = aliexpressDF.withColumn("soldValue", (col("price") * col("sold")) / 1000)
      val categorySoldValue: Dataset[Row]  = withSoldValue
        .groupBy("category_name")
        .agg(sum("soldValue")
        .alias("soldValueThousandUSD"))
        .sort(desc("soldValueThousandUSD"))
        .withColumn("soldValueThousandUSD", round(col("soldValueThousandUSD"), 0))
        categorySoldValue
    }

    /**
     *
     * @param aliexpressDF cleaned DataFRme
     * @return datafreme with additional column "auction age in days"
     */

    def daysFromAuctionLaunch(aliexpressDF: Dataset[Row] ): Dataset[Row]  = {
      val withDiffDate = aliexpressDF.select(
        col("lunchTime"),
        current_date().as("current_date"),
        datediff(current_date(), col("lunchTime")).as("datediffDays")
      )
      withDiffDate
    }

    /**
     *
     * @param aliexpressDF cleaned DataFrame
     * @return Dataframe with calculated sold value per day "title","lunchTime","soldValue","datediff","valuePerDay"
     */

    def soldValuePerDay(aliexpressDF: Dataset[Row]): Dataset[Row] = {
      val withSoldValue = aliexpressDF
      .withColumn("soldValue", col("price") * col("sold"))
      .withColumn("datediff",datediff(current_date(), col("lunchTime")))
      .withColumn("valuePerDay",col("soldValue") / col("datediff"))  //sold per days
      .withColumn("valuePerDay",round(col("valuePerDay"),2))

      val columns = List("title","lunchTime","soldValue","datediff","valuePerDay")
      withSoldValue.select(columns.map(m => col(m)): _*)  //selecting columns

    }




}
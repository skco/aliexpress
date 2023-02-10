package aliexpress
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Aliexpress {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("aliexpress")
      .master("local[*]")
      .getOrCreate()

    val loader: AliexpressLoader     = new AliexpressLoader
    val cleaner: AliexpressCleaner   = new AliexpressCleaner
    val analyser: AliexpressAnalyzer = new AliexpressAnalyzer
    val reportWriter : AliexpressReportWriter  = new AliexpressReportWriter

    //loader & clean
    val aliexpressDF:Dataset[Row] = cleaner.cleanAliexpressDataset(spark,loader.loadAliexpressDataset(spark,".\\dataset\\aliexpress.csv"))

    //analyzer

    val itemInCategories = analyser.countItemsInCategories(spark,aliexpressDF)
    val soldValuePerDay = analyser.soldValuePerDay(spark, aliexpressDF).sort()
    val totalSoldValuebyCat  =analyser.calculateTotalSoldValueByCategory(spark, aliexpressDF)
    val bestsellersByQty = aliexpressDF.sort(desc("sold"))
    val daysFromAuctionStart = analyser.daysFromAuctionLaunch(spark,aliexpressDF)

    reportWriter.writeCsvReport(spark,itemInCategories,"itemInCategories")
    reportWriter.writeCsvReport(spark,soldValuePerDay,"soldValuePerDay")
    reportWriter.writeCsvReport(spark,totalSoldValuebyCat,"totalSoldValuebyCat")
    reportWriter.writeCsvReport(spark,bestsellersByQty,"bestsellersByQty")
    reportWriter.writeCsvReport(spark,daysFromAuctionStart,"daysFromAuctionStart")
  }

}

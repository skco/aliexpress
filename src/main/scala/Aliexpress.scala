import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Aliexpress {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("aliexpress")
      //.master("local[*]")
      .getOrCreate()

    val loader: AliexpressLoader     = new AliexpressLoader
    val cleaner: AliexpressCleaner   = new AliexpressCleaner
    val analyser: AliexpressAnalyzer = new AliexpressAnalyzer
    val reportWriter : AliexpressReportWriter  = new AliexpressReportWriter

    //loader & clean
    val aliexpressDF:Dataset[Row] = cleaner.cleanAliexpressDataset(spark,loader.loadAliexpressDataset(spark,"hdfs://hadoopMaster:9000/aliexpress/aliexpress.csv"))

    //analyzer

    val itemInCategories = analyser.countItemsInCategories(spark,aliexpressDF)
    val soldValuePerDay = analyser.soldValuePerDay(spark, aliexpressDF).sort()
    val totalSoldValuebyCat  = analyser.calculateTotalSoldValueByCategory(spark, aliexpressDF)
    val bestsellersByQty = aliexpressDF.sort(desc("sold"))
    val daysFromAuctionStart = analyser.daysFromAuctionLaunch(spark,aliexpressDF)

    reportWriter.writeCsvReport(spark,itemInCategories,"hdfs://hadoopMaster:9000/aliexpress/itemInCategories")
    reportWriter.writeCsvReport(spark,soldValuePerDay,"hdfs://hadoopMaster:9000/aliexpress/soldValuePerDay")
    reportWriter.writeCsvReport(spark,totalSoldValuebyCat,"hdfs://hadoopMaster:9000/aliexpress/totalSoldValuebyCat")
    reportWriter.writeCsvReport(spark,bestsellersByQty,"hdfs://hadoopMaster:9000/aliexpress/bestsellersByQty")
    reportWriter.writeCsvReport(spark,daysFromAuctionStart,"hdfs://hadoopMaster:9000/aliexpress/daysFromAuctionStart")
  }

}

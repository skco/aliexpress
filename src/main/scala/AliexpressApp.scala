import org.apache.spark.sql.{ Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object AliexpressApp{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("AliexpressApp")
      //.master("local[*]")
      .getOrCreate()

    val HDFSPath = "hdfs://hadoopMaster:9000/aliexpress/"

    val loader: AliexpressLoader     = new AliexpressLoader
    val cleaner: AliexpressCleaner   = new AliexpressCleaner
    val analyser: AliexpressAnalyzer = new AliexpressAnalyzer
    val reportWriter : AliexpressReportWriter  = new AliexpressReportWriter

    //loader & clean
    val aliexpressDF:Dataset[Row] = cleaner.cleanAliexpressDataset(loader.loadAliexpressDataset(spark,HDFSPath+"aliexpress.csv"))

    //analyzer
    val itemInCategories     = analyser.countItemsInCategories(aliexpressDF)
    val soldValuePerDay      = analyser.soldValuePerDay(aliexpressDF).sort()
    val totalSoldValueByCat  = analyser.calculateTotalSoldValueByCategory(aliexpressDF)
    val daysFromAuctionStart = analyser.daysFromAuctionLaunch(aliexpressDF)
    val bestsellersByQty     = aliexpressDF.sort(desc("sold"))

    reportWriter.writeCsvReport(itemInCategories,    HDFSPath+"itemInCategories")
    reportWriter.writeCsvReport(soldValuePerDay,     HDFSPath+"soldValuePerDay")
    reportWriter.writeCsvReport(totalSoldValueByCat, HDFSPath+"totalSoldValueByCat")
    reportWriter.writeCsvReport(bestsellersByQty,    HDFSPath+"bestsellersByQty")
    reportWriter.writeCsvReport(daysFromAuctionStart,HDFSPath+"daysFromAuctionStart")
  }

}

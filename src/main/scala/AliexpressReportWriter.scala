import org.apache.spark.sql.{ DataFrame,Dataset,Row,SaveMode}

class AliexpressReportWriter{
  /**
   *
   * @param aliexpressDF analyzer output
   * @param fileName csv filename
   */
  def writeCsvReport(aliexpressDF:Dataset[Row],fileName:String):Unit =  {
        aliexpressDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", value = true).csv(fileName)
  }
}
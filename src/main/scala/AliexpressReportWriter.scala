import org.apache.spark.sql.{ DataFrame, SaveMode}

class AliexpressReportWriter{
  /**
   *
   * @param aliexpressDF analyzer output
   * @param fileName csv filename
   */
  def writeCsvReport(aliexpressDF:DataFrame,fileName:String):Unit =  {
        aliexpressDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", value = true).csv(fileName)
  }

}
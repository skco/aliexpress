import org.apache.spark.sql.{ DataFrame, SaveMode}

class AliexpressReportWriter{
  def writeCsvReport(aliexpressDF:DataFrame,fileName:String):Unit =  {
        aliexpressDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", value = true).csv(fileName)
  }

}
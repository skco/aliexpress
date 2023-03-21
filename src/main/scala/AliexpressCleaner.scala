import org.apache.spark.sql.DataFrame

  class AliexpressCleaner {
    def cleanAliexpressDataset(aliexpressDF:DataFrame): DataFrame = {
      aliexpressDF.drop("category")  // one value in whole column
    }

}
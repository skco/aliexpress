import org.apache.spark.sql.DataFrame

  class AliexpressCleaner {

    /**
     *
     * @param aliexpressDF DataFrame from loader output loader
     * @return cleaned DataFrame
     */
    def cleanAliexpressDataset(aliexpressDF:DataFrame): DataFrame = {
      aliexpressDF.drop("category")  // one value in whole column
    }

}
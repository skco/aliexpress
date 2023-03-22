import org.apache.spark.sql.{DataFrame, Dataset, Row}}
  class AliexpressCleaner {

    /**
     *
     * @param aliexpressDF DataFrame from loader output loader
     * @return cleaned DataFrame
     */
    def cleanAliexpressDataset(aliexpressDF:Dataset[Row]): Dataset[Row] = {
      aliexpressDF.drop("category")  // one value in whole column
    }

}
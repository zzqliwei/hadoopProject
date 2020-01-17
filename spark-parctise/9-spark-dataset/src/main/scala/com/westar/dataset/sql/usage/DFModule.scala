package com.westar.dataset.sql.usage

import org.apache.spark.sql.DataFrame

trait DFModule {
  def cal(personDf: DataFrame): DataFrame = {
    if (personDf.schema.contains("age")) {
      personDf.select("age")
    } else {
      personDf.select("name")
    }


  }
}

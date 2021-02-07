package com.mymall.analytics.schema

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Contains schema of Customer Info data set
  */
object CustomerInfo {
  def getSchema:StructType = {
    new StructType(Array(
      StructField("customerId", DataTypes.StringType, true),
      StructField("gender", DataTypes.StringType, true),
      StructField("age", DataTypes.StringType, true),
      StructField("income", DataTypes.StringType, true),
      StructField("spendingScore", DataTypes.StringType, true)
    ))
  }
}

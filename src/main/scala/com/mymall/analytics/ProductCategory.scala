package com.mymall.analytics

import com.mymall.analytics.schema.CustomerInfo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

object ProductCategory {

  def main(args: Array[String]): Unit = {

    val path = System.getProperty("user.dir") + "/src/main/resources/Worksheet_Kafka_assignment_Mall_Customers.csv"
    println(path)

    val conf = new SparkConf()
    conf.setAppName("kafka_reader-1.0").setMaster("local[*]").setAppName("StreamPersonInfo")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    import spark.implicits._

    val kafkaDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "person_info")
      .load()
      .select("key", "value")

    kafkaDf.printSchema()

    val personInfo = kafkaDf.withColumn("_tmp", split($"value", ",")).select(
      $"_tmp".getItem(0).as("personId"),
      $"_tmp".getItem(1).as("distance"),
      $"_tmp".getItem(2).as("mallId"),
      $"key".cast(DataTypes.StringType).as("clientTimestamp")
    )

    personInfo.printSchema()
    personInfo.createOrReplaceTempView("personInfo")

    val customerInfo = spark
      .read
      .schema(CustomerInfo.getSchema)
      .csv(path)
    customerInfo.printSchema()
    customerInfo.createOrReplaceTempView("customerInfo")

    val finalCustomers = spark.sql(
      """
        |select
        |*
        |from personInfo
        |inner join customerInfo
        |   on personInfo.personId = customerInfo.customerId
        |   having distance < 100
      """.stripMargin)

    finalCustomers.createOrReplaceTempView("finalCustomers")

    val productCategory = udf[String, String, String, String, String] {
      getProductCategory
    }
    spark.udf.register("productCategory", productCategory)

    val customerWithProductCategory = spark.sql(
      """
        |select
        |*, productCategory(gender,age,income,spendingScore) as productCategory
        |from finalCustomers
      """.stripMargin)

    val query = customerWithProductCategory.writeStream
      .format("console")
      .start()

    query.awaitTermination()
  }

  def getProductCategory(gender: String, age: String, income: String, spendingScore: String): String = {
    null
  }
}

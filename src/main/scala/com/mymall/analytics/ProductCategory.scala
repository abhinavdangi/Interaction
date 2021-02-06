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
    conf.setAppName("kafka_reader-1.0").setMaster("local[*]").setAppName("StreamPersonData")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    import spark.implicits._

    val kafkaDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "person_info")
      .option("failOnDataLoss","false")
      .load()
      .select("key", "value")

    kafkaDf.printSchema()

    val personInfo = kafkaDf.withColumn("_tmp", split($"value", ",")).select(
      $"_tmp".getItem(0).as("customerId"),
      $"_tmp".getItem(1).as("distance"),
      $"_tmp".getItem(2).as("mallId"),
      $"key".cast(DataTypes.StringType).as("clientTimestamp")
    ).where("distance < 100")

    personInfo.printSchema()

    val customerInfo = spark
      .read
      .schema(CustomerInfo.getSchema)
      .csv(path)
    customerInfo.printSchema()

    val finalCustomers = personInfo.join(customerInfo, Seq("customerId"))

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

    val query = customerWithProductCategory
      .selectExpr("CAST(customerId AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/tmp/myMall/analytics/interaction")
      .option("topic", "person_product_category")
      .start()

    query.awaitTermination()
  }

  def getProductCategory(gender: String, age: String, income: String, spendingScore: String): String = {
    if("Male".equals(gender)){
      if(Integer.parseInt(income)*Integer.parseInt(spendingScore)/100 > 50){
        "Men's Watches and Fashion Accessories"
      } else {
        if(Integer.parseInt(age) < 30){
          "Men's Sports and Active wear"
        } else {
          "Men's Festive wear"
        }
      }
    } else {
      if(Integer.parseInt(income)*Integer.parseInt(spendingScore)/100 > 50){
        "Women Handbags and Jewellery"
      } else {
        if(Integer.parseInt(age) < 30){
          "Women's Western wear"
        } else {
          "Women's Ethnic wear"
        }
      }
    }
  }
}

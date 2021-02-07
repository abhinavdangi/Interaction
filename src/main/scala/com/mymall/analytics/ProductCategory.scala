package com.mymall.analytics

import java.time
import java.time.LocalDateTime

import com.mymall.analytics.constants.ConsumerConstants
import com.mymall.analytics.schema.CustomerInfo
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Processes the data coming in from kafka, and puts back in a different topic. Structured
  * streaming is used for processing data because it can handle scale with ease.
  *
  * The input person data is filtered based on distance and joined with customer data.
  * Finally, product category is assigned for each person.
  */
object ProductCategory {

  var date: LocalDateTime = LocalDateTime.now()

  /**
    * Refreshes static customerInfo DF daily
    * @param spark SparkSession
    * @param customerInfo DF
    */
  def refreshCustomerInfo(spark: SparkSession, customerInfo: DataFrame): Unit = {
    val currentDate = LocalDateTime.now()
    val duration = time.Duration.between(currentDate, date)
    if (duration.toDays > 1) {
      customerInfo.unpersist()
    }
    customerInfo.cache()
    customerInfo.count()
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("kafka_reader-1.0").setMaster("local[*]").setAppName("StreamPersonData")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    val path = System.getProperty("user.dir") + "/src/main/resources/Worksheet_Kafka_assignment_Mall_Customers.csv"
    println(path)

    val customerInfo = spark
      .read
      .schema(CustomerInfo.getSchema)
      .csv(path)

    import spark.implicits._

    val kafkaDf = spark
      .readStream
      .format(ConsumerConstants.CONSUMER_KAFKA)
      .option(ConsumerConstants.KAFKA_BOOTSTRAP_SERVERS, ConsumerConstants.KAFKA_BOOTSTRAP_SERVERS_DEFAULT)
      .option(ConsumerConstants.KAFKA_SUBSCRIBE, "person_info")
      .option(ConsumerConstants.FAIL_ON_DATA_LOSS,ConsumerConstants.BOOLEAN_FALSE)
      .load()
      .select("key", "value")

    kafkaDf.printSchema()

    val personInfo = kafkaDf.withColumn("_tmp", split($"value", ",")).select(
      $"_tmp".getItem(0).as("customerId"),
      $"_tmp".getItem(1).as("xCoordinate"),
      $"_tmp".getItem(2).as("yCoordinate"),
      $"key".cast(DataTypes.StringType).as("clientTimestamp")
    ).where("pow(100-xCoordinate,2) + pow(100-yCoordinate,2) < pow(100,2)")

    personInfo.printSchema()

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

    val stream = query
      .writeStream
      .format(ConsumerConstants.PRODUCER_KAFKA)
      .option(ConsumerConstants.KAFKA_BOOTSTRAP_SERVERS, ConsumerConstants.KAFKA_BOOTSTRAP_SERVERS_DEFAULT)
      .option("checkpointLocation", "/tmp/myMall/analytics/interaction")
      .option("topic", "person_product_category")
      .start()

    stream.awaitTermination()
  }

  /**
    * Udf for getting product category based on input data
    * @param gender Male, Female
    * @param age Age of the person
    * @param income Income of the person
    * @param spendingScore Spending as a percent of income of the person
    * @return Product category
    */
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

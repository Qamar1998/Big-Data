package all
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession

import com.mongodb.spark.config._
import org.bson.Document
import scala.util.parsing.json
import org.apache.spark.sql.{DataFrame, Encoder}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, date_format,lit, to_date, to_timestamp, unix_timestamp}
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.hadoop.fs.shell.Count
import com.mongodb.spark.config._
import com.mongodb.spark._
import scala.util.parsing.json.JSONObject
import org.apache.spark.sql.types.DateType
import java.util.Date

import org.apache.spark.sql.functions.udf
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.mongodb.scala._
import org.mongodb.scala.model.geojson._
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.model.Filters

import com.mongodb.client.model.Projections



object geolocated_tweets extends App {


  // Using BasicConfigurator to stop INFO messages displaying on spark console
  val nullAppender = new NullAppender
  BasicConfigurator.configure(nullAppender)

  // Create a Spark session
  val spark = SparkSession
    .builder()
    .appName("mongoSparkConnectorIntro")
    .master("local")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mongoDB.tweets")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mongoDB.tweets")
    .getOrCreate()

  val sc = spark.sparkContext

  // Set directory path for the input data file
  val path = "src/main/resources/geolocated_tweets.json"

  // Requirement 1 : Use Spark DataFrame reader to read the input data(.json) file as a Spark DataFrame
  val df = spark
    .read
    .format("json")
    .option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "true")
    .load(path)

/*
  // Convert time in "created_at" col to date time as d/m/y
  import org.apache.spark.sql.expressions.UserDefinedFunction

  import org.apache.spark.sql.functions.{col, udf}
  import spark.implicits._


  val time = (input: String) => {
    val f = DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss z uuuu").withLocale(Locale.US)
    val zdt = ZonedDateTime.parse(input, f)

    val ld = zdt.toLocalDate
    val fLocalDate = DateTimeFormatter.ofPattern("dd/MM/uuuu")
    val output = ld.format(fLocalDate)
  }

  val to_date = udf(time)

  // Apply the UDF to change the source dataset
  df.withColumn("DATE", to_date('created_at))

 */




  // Use the WriteConfig to inform Mongo that we want to append documents to the collection.
  val conf = WriteConfig(Map("collection"-> "tweets", "writeConcern.w"-> "majority"),Some(WriteConfig(sc)))
  MongoSpark.save(df,conf)

  df.withColumn("dateType",col("created_at")).show(10)


  // Connect to MongoDB deployment running on localhost
  val mongoClient: MongoClient = MongoClient()
  // Define database to refer to the mongoDB database
  val database: MongoDatabase = mongoClient.getDatabase("mongoDB")
  // Get Collection from DB
  val collection = database.getCollection("tweets")


  // Requirement 3:  create the 2d Index on geo-location
  collection.createIndex(Indexes.geo2d("coordinates.coordinates"))

  //Requirement 4 : Apply way 1 mongoDB filtering
  // Spatial filtering
  val between_lon_lat = collection.find(Filters.geoWithinCenter("coordinates.coordinates", 34.1 , -118.0, 100.0 )).projection(Projections
    .fields(Projections.include("text", "created_at"), Projections.excludeId()))

  between_lon_lat.foreach(println)

  // Time filtering



}

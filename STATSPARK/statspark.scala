// Databricks notebook source
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter

val formatteryyyy = DateTimeFormatter.ofPattern("yyyy")
val yesterday = ZonedDateTime.now(ZoneId.of("America/New_York")).minusDays(1)
val yyyy = formatteryyyy format yesterday
println(yyyy)
val formattermm = DateTimeFormatter.ofPattern("MM")
val mm = formattermm format yesterday
println(mm)
val formatterdd = DateTimeFormatter.ofPattern("dd")
val dd = formatterdd format yesterday
println(dd)


println("Hello World RentPath Big Data Team!")

    //Configuration Resource site:
    //https://spark.apache.org/docs/latest/configuration.html
    val spark = SparkSession.builder
      .appName("RentPath-BDT-StatFlatten")
      .enableHiveSupport
      .getOrCreate



    //Logging Level
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val cal = Calendar.getInstance
    cal.add (Calendar.DATE, -1)
    val dateFormat = new SimpleDateFormat ("yyyyMMdd")

    //val inputDate = "20180626"//args(0)
    val inputDate ="yyyy="+yyyy+"/mm="+mm+"/dd="+dd
    
    println(inputDate)
    //if(inputDate.length < 8) {
    //  dateFormat.format(cal.getTime)
    //}
    //val adlLocation = "rentpathdatalake.azuredatalakestore.net/PROD/API-STAT/INPUT/" + inputDate + "/Bulk/JSON/"
    //val adlOutputLocation = "adl://" + "rentpathdatalake.azuredatalakestore.net/PROD/API-STAT/Bulk/"
    val adlOutputLocation = "dbfs:/mnt/adl/PROD/Output/SEOKeywordRanks/SERPS/"
    //val location = "adl://" + adlLocation + "serps" + inputDate + ".json"
    val location = "dbfs:/mnt/adl/PROD/Input/STAT/SERPS/brand=aptg/yyyy=2018/mm=06/dd=26/serps20180626.json.gz"
    println(">>>>>>>>>> Location: " + location)
    val statInitDF = spark.read.json(location).cache
    println(">>>>>>>>>> Location: " + location + " Cached")

    //statInitDF = statInitDF.limit(5)
    //statInitDF.printSchema

    val res = statInitDF.select(explode($"Response.sites.KeywordSerps"))
    val res2 = res.select(explode($"col"))

    val keyword = res2.select("col.keyword_id", "col.device", "col.keyword", "col.location", "col.market")
    val google = res2.select(explode($"col.google"), $"col.keyword_id")
    val google2 = google.select($"col.BaseRank",$"col.Protocol",$"col.Rank",$"col.ResultTypes.ResultType",$"col.Url",$"keyword_id").withColumn("searchengine", lit("google"))
    //val yahoo = res2.select(explode($"col.yahoo"), $"col.keyword_id")
    //val yahoo2 = yahoo.select($"col.BaseRank",$"col.Protocol",$"col.Rank",$"col.ResultTypes.ResultType",$"col.Url",$"keyword_id").withColumn("searchengine", lit("yahoo"))
    //val bing = res2.select(explode($"col.bing"), $"col.keyword_id")
    //val bing2 = bing.select($"col.BaseRank",$"col.Protocol",$"col.Rank",$"col.ResultTypes.ResultType",$"col.Url",$"keyword_id").withColumn("searchengine", lit("bing"))


    val google3 = keyword.join(google2, Seq("keyword_id"), "left")
    //val yahoo3 = keyword.join(yahoo2, Seq("keyword_id"), "left")
    //val bing3 = keyword.join(bing2, Seq("keyword_id"), "left")

    val unionAll = google3.persist()
    //unionAll.printSchema
    println(">>>>>>>>>> Location: " + location + " Persisted")

    val adlOutputLocationCSV = adlOutputLocation + "csv/"+ inputDate + "/"
    val adlOutputLocationParquet = adlOutputLocation + "parquet/"+ inputDate + "/"
    val adlOutputLocationJSON = adlOutputLocation + "JSON/"+ inputDate + "/"

    println(">>>>> writing CSV output to " + adlOutputLocationCSV)
    unionAll.coalesce(5).write.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").mode("Overwrite").save(adlOutputLocationCSV)
    println(">>>>> Done writing CSV")
    println(">>>>> writing JSON output to " + adlOutputLocationJSON)
    unionAll.coalesce(5).write.mode("Overwrite").json(adlOutputLocationJSON)
    println(">>>>> Done writing JSON")
    println(">>>>> writing Parquet output to " + adlOutputLocationParquet)
    unionAll.coalesce(5).write.mode("Overwrite").parquet(adlOutputLocationParquet)
    println(">>>>> Done writing Parquet")
    println(">>>>> ----------------------------------")
    //spark.stop()

// COMMAND ----------



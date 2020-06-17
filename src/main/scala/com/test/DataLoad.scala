package com.test

import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.parsing.json.JSONObject

object DataLoad{
  def main(args: String): Unit = {
    val t1 = System.nanoTime
    println("Processing started....... ")
    //val log = LogManager.getRootLogger
    //log.setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("Load_Data_From_CSV_To_MongoDB")
      .master("local")
      .config("spark.mongodb.input.uri","mongodb://localhost:27017/org.testTable")
      .config("spark.mongodb.output.uri","mongodb://localhost:27017/org.testTable")
      .getOrCreate()

    val sc = spark.sparkContext
    //sc.setLogLevel("warning")
    if (args.length == 0) {
      println("\nPlease provide product file location\n")
      System.exit(1)
    }
    val path = args

    val dataDF = spark
      .read
      .format("csv")
      .option("header",true)
      .option("sep",",")
      .option("inferSchema",true)
      .load(path)
    val mongoClient: MongoClient = new MongoClient("localhost",27017)
    val database: MongoDatabase = mongoClient.getDatabase("org")
    val collection: MongoCollection[Document] = database.getCollection("testTable")
    val writeConfig = WriteConfig(Map("collection"->"testTable",
      "writeConcern.w"->"majority"),Some(WriteConfig(sc)))

    var m1: Map[String, String] = Map()
    m1 = m1 ++ Map("sku"->"text")
    val js1 = JSONObject(m1).toString()
    val parsedJs = Document.parse(js1)
   collection.createIndex(parsedJs)


    val sparkDocs = dataDF.rdd.mapPartitions(partition=>{
      val docs = partition.map(row =>{
        var colMap : Map[String,String] = Map()
        row.schema.map(_.name).map(col => {
          val colName = col.trim
          val colIndex = row.fieldIndex(colName)
          val colAnyValue = row.getAs[AnyVal](colIndex)
          var colVal = if (colAnyValue == null) "NULL" else colAnyValue.toString()
          if(colVal==null)
          {
            val nullCol = "NULL"
            colVal = nullCol.asInstanceOf[String]
          }
          if(colVal.toString() == "NULL"){
            print("do nothing")
          }
          else{
            colMap = colMap ++ Map(colName->colVal)
          }
        })
        val json = JSONObject(colMap).toString()
        val parsedJson = Document.parse(json)
        parsedJson
      }).toIterator
      docs
    })
    MongoSpark.save(sparkDocs,writeConfig)
    println("Processing completed ...............")
    val duration = (System.nanoTime - t1) / 1e9d

    print("Total Time Taken : "+duration+" seconds\n")
    return duration
  }
}

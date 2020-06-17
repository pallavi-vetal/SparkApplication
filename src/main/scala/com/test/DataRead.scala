package com.test

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

object DataRead {
  def main(args: String): MongoRDD[Document] = {
    val spark = SparkSession.builder()
      .appName("mongozips")
      .master("local[*]")
      .config("spark.mongodb.input.uri","mongodb://localhost:27017/org.testTable")
      .config("spark.mongodb.output.uri","mongodb://localhost:27017/org.testTable")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd = MongoSpark.load(sc)
    if (args.length == 0) {
      println("\nPlease provide product file location\n")
      System.exit(1)
    }
    val name = args


    //Find by Name
    val queryJson = "{ $match: { name : '"+name+"' } }"

    val aggregatedRdd = rdd.withPipeline(
      List(
        Document.parse(queryJson)));

    return aggregatedRdd
  }

}

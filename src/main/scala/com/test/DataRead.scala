package com.test


import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

object DataRead {
  def main(sc:SparkContext, value: String, fieldName:String): MongoRDD[Document] = {

    val rdd = MongoSpark.load(sc)

    val queryJson = "{ $match: { "+fieldName+" : '"+value+"' } }"

    val aggregatedRdd = rdd.withPipeline(
      List(
        Document.parse(queryJson)));

    return aggregatedRdd
  }

}

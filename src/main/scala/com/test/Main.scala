package com.test

import org.apache.spark.sql.SparkSession

object Main extends  App {
  override def main(args: Array[String]): Unit = {
    println("*********************************")

    val spark = SparkSession.builder()
      .appName("mongozips")
      .master("local[*]")
      .config("mongo.job.input.format","com.mongodb.hadoop.MongoInputFormat")
      .config("spark.mongodb.input.uri","mongodb://localhost:27017/org.testTable")
      .config("spark.mongodb.output.uri","mongodb://localhost:27017/org.testTable")
      .getOrCreate()
    val sc = spark.sparkContext
      var opInput = args(0)
      opInput match {
          case "1" =>{
            println("Enter file path of csv")
            val path = readLine()
            val processing_time = DataLoad.main(spark,path.toString)
            println("\nTime taken for loading data : "+processing_time+" seconds")
          }
          case "2" =>{
            println("\n********************************\n")
            println("Search function\n: please specify field name you want to search and its value\n")
            println("\nFor example: If you want to search by name :  input will look like name=pallavi")
            println("\n********************************")

            val query = readLine().toString
            val res = query.split("=").toList
            val fieldName = res(0)
            val value= res(1)

            val t1 = System.nanoTime
            println("Processing started....... ")
            val rdd = DataRead.main(sc,value,fieldName)
            val count = rdd.count()
            println("\nTotal records found : "+count)
            if(rdd.count()>0)
                 println("\nFirst Record \n"+rdd.first())
            val duration = (System.nanoTime - t1) / 1e9d
            print("Total Time Taken : "+duration+" seconds\n")
          }
          case "3" =>{
            val t1 = System.nanoTime
            println("\n*************************************\n")
            println("Notes:\n1.Function taked sku as where clause and asks for values to update.\n2." +
              "If you want to update field value ,press enter without specifying value\n\n")
            println("Enter SKU by which you want to perform update operation")

            val sku = readLine()
            println("Enter new sku value")
            val newsku = readLine()
            println("Enter new product name")
            val name = readLine()

            println("Enter new description ")
            val desc = readLine()
            println("Processing started....... ")
            DataUpdateBySKU.main(sku.toString,name.toString,desc.toString,newsku.toString)

            val duration = (System.nanoTime - t1) / 1e9d
            print("Total Time Taken : "+duration+" seconds\n")


          }
          case "4" =>{
            System.exit(0)
          }



    }

  }
}

package com.test

object Main extends  App {
  override def main(args: Array[String]): Unit = {
    println("*********************************")

    while(true){
      println("1. Load Data from csv to MongoDB\n2. Aggregate query on table.\n3. Update table based on SKU\n4. Exit\n")
      println("Make choice : ")
      var opInput = readLine()
      opInput match {
          case "1" =>{
            println("Enter file path of csv")
            val path = readLine()
            val processing_time = DataLoad.main(path)
            println("\nTime taken for loading data : "+processing_time+" seconds")
          }
          case "2" =>{
            println("Enter product name to search")
            val name = readLine()
            val t1 = System.nanoTime
            val rdd = DataRead.main(name)
            println("\nTotal records found with name "+name+"  : "+rdd.count())
            println("\nFirst Record \n"+rdd.first())
            val duration = (System.nanoTime - t1) / 1e9d
            print("Total Time Taken : "+duration+" seconds\n")
          }
          case "3" =>{
            println("Enter file path of csv")
            val path = readLine()
            DataLoad.main(path)
          }
          case "4" =>{
            System.exit(0)
          }
      }


    }

  }
}

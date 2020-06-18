package  com.test
import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.spark._

import org.apache.spark.sql.SparkSession
import org.bson.Document




object DataLoad{
  def main(spark:SparkSession,args: String): Unit = {
    val t1 = System.nanoTime
    println("Processing started....... ")


    val sc = spark.sparkContext

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

    import com.mongodb.spark.config._

    import scala.util.parsing.json.JSONObject
    val mongoClient: MongoClient = new MongoClient("127.0.0.1",27017)
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
    import com.mongodb.spark.config._
    MongoSpark.save(sparkDocs,writeConfig)
    println("Processing completed ...............")
    val duration = (System.nanoTime - t1) / 1e9d

    print("Total Time Taken : "+duration+" seconds\n")
    null
  }
}
/*
spark-submit --deploy-mode client  --class DataRead --jars=`find /Users/vetalpallavi21/docker-spark-cluster/apps -print | grep -i '.*[.]jar' | tr '\n' ','` /Users/vetalpallavi21/Scala/TestDataLoadV.0.2/target/TestDataLoadV.0.2-1.0-SNAPSHOT.jar
spark-submit --deploy-mode client  --class DataLoad --jars=`find /Users/vetalpallavi21/docker-spark-cluster/apps -print | grep -i '.*[.]jar' | tr '\n' ','`  /Users/vetalpallavi21/Scala/TestDataLoadV.0.2/target/TestDataLoadV.0.2-1.0-SNAPSHOT.jar
db.testTable.update(
   { sku: "more-car-those" },
   { $set:
      {
        name: "pallavi"
      }
   },
   {multi:true}
)
 */
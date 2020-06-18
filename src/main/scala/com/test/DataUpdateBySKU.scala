package com.test

import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}

object DataUpdateBySKU {
  def main(sku:String,name:String,desc:String,newSku:String):Unit = {
     //db.testTable.updateOne({sku:'Timothy Howell'},{$set:{name:'pallavi'} })
    var js1="{$set:{"

    if(name!="")
       js1+="name:'"+name+"'"
    if(desc!="")
     js1+=",description:'"+desc+"'"
    if(sku!="")
      js1+=",sku:'"+newSku+"'"
    js1+="}}"
    if(js1=="{$set:{}}")
      {
        println("Please specify at least one update field.\nNow exiting")
        System.exit(1)
      }
    val mongoClient: MongoClient = new MongoClient("127.0.0.1",27017)
    val database: MongoDatabase = mongoClient.getDatabase("org")
    val collection: MongoCollection[Document] = database.getCollection("testTable")
    val filter : Bson = new Document("sku", sku)


   val updateOperationDocument = BsonDocument.parse(js1)
    val res = collection.updateMany(filter, updateOperationDocument)
      res
  }
}

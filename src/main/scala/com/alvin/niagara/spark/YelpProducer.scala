package com.alvin.niagara.spark

import com.alvin.niagara.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.HashMap

import com.alvin.niagara.model._
import com.alvin.niagara.util.{AvroObjectProducer, AvroProducer}

/**
  * Created by alvinjin on 2017-03-12.
  */
object YelpProducer extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("Spark Batch App")
    .getOrCreate()


  import spark.implicits._

  val businessPath = yelpInputPath+"yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath+"yelp_academic_dataset_review.json"
  val tipPath = yelpInputPath+"yelp_academic_dataset_tip.json"
  val userPath = yelpInputPath+"yelp_academic_dataset_user.json"
  val checkinPath = yelpInputPath+"yelp_academic_dataset_checkin.json"

  try {

    val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
    val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
    val tipDS: Dataset[Tip] = spark.read.json(tipPath).as[Tip]
    val userDS: Dataset[User] = spark.read.json(userPath).as[User]
    val checkinDS: Dataset[Checkin] = spark.read.json(checkinPath).as[Checkin]



   /* businessDS.take(10).map(x => println(x.address))
    reviewDS.take(10).map(x => println(x.`type`))
    tipDS.take(10).map(x => println(x.likes))
    userDS.take(10).map(x => println(x.compliment_cool))
    checkinDS.take(10).map(x => println(x.time))
*/
   val kafkaOpTopic = "test"
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

    //val producer = new AvroObjectProducer(checkinTopic)

    checkinDS.take(100).map{ r =>
      val msg = CheckinSerde.serialize(r)
      val key = r.business_id
      //producer.send(key, msg)



        val data = key
        // As as debugging technique, users can write to DBFS to verify that records are being written out
        // dbutils.fs.put("/tmp/test_kafka_output",data,true)
        val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
        producer.send(message)


    }
//without close(), can't send data actually
  producer.close()

    import spark.implicits
   // businessDF.take(5).map(println)
   // reviewDF.take(5).map(println)
   // tipDF.take(5).map(println)
   // userDF.take(5).map(println)
   // checkinDF.take(5).map(println)

   // println(businessDF.schema.json)//printSchema()
   // println(reviewDF.schema.json)//.printSchema()
     // println( tipDF.schema.json)//printSchema()
     // println( userDF.schema.json)//printSchema()
     // println( checkinDF.schema.json)//printSchema()






 } finally {
    spark.stop()
  }


}

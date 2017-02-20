package com.alvin.niagara.akkastreams

/**
  * Created by alvinjin on 2017-02-17.
  */

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.util.concurrent.atomic.AtomicLong

import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import com.alvin.niagara.cassandra.CassandraConfig
import com.alvin.niagara.common.{Post, Setting}
import com.datastax.driver.core.{Cluster, PreparedStatement}

trait AkkaConsumer extends Setting {

  val system = ActorSystem("AkkaConsumer")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val maxPartitions = 100

  // #settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(brokerList)
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  //#settings

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers(brokerList)

  val kafkaProducer = producerSettings.createKafkaProducer()

  implicit val session = Cluster.builder.addContactPoint(CassandraConfig.hosts(0)).withPort(CassandraConfig.port).build.connect()

  def business[T] = Flow[T]

  // #db
  class DB {

    private val aoffset = new AtomicLong

    def save(offset: Long, post: Post): Future[Done] = {
      println(s"DB.save: ${post.postid}")
      aoffset.set(offset)
      Future.successful(Done)
    }

    def loadOffset(): Future[Long] =
      Future.successful(aoffset.get)

    def update(data: String): Future[Done] = {
      println(s"DB.update: $data")
      Future.successful(Done)
    }
  }

  // #db

  // #rocket
  class Rocket {
    def launch(destination: String): Future[Done] = {
      println(s"Rocket launched to $destination")
      Future.successful(Done)
    }
  }

  // #rocket

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}

// Consume messages and store a representation, including offset, in DB
object ExternalOffsetStorageAkka extends AkkaConsumer {
  def main(args: Array[String]): Unit = {


    val preparedStatement = session.prepare("INSERT INTO test.post5(typeid) VALUES (?)")
    val statementBinder = (typeId: Integer, statement: PreparedStatement) => statement.bind(typeId)
    val cassandraSink = CassandraSink[Integer](parallelism = 2, preparedStatement, statementBinder)

    /*
    val db = new DB
    db.loadOffset().foreach { fromOffset =>
                val partition = 0
                val subscription = Subscriptions.assignmentWithOffset(new TopicPartition(topic, partition) -> fromOffset)
                val done = Consumer.plainSource(consumerSettings, subscription)
                   .map(msg => (Post.deserializeToClass(msg.value()), msg.offset()))
               .mapAsync(1){case (post, offset) =>
                         db.save(offset, post)
                 }

                  .map(msg => Post.deserializeToClass(msg.value()))
                  .map(post => post.typeid:Integer)
                //  .runWith(Sink.ignore)
                  .runWith(sink)

                terminateWhenDone(done)
    }*/


    val done = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
        .map(msg => Post.deserializeToClass(msg.record.value()))
        .map{post =>  println(post.typeid)
          post.typeid: Integer}
        .runWith(cassandraSink)

    terminateWhenDone(done)
  }
}


/*
object AkkaConsumerToProducerFlow extends AkkaConsumer {
  def main(args: Array[String]): Unit = {


    val preparedStatement = session.prepare("INSERT INTO akka_stream_scala_test.test(id) VALUES (?)")
    val statementBinder = (myInteger: Integer, statement: PreparedStatement) => statement.bind(myInteger)
    val sink = CassandraSink[Integer](parallelism = 2,preparedStatement, statementBinder)

    // #consumerToProducerFlow
    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
        .map { msg =>

          ProducerMessage.Message(new ProducerRecord[String, Array[Byte]]("topic2", msg.record.value), msg.committableOffset)
        }
        .via(Producer.flow(producerSettings))
        .mapAsync(1) { msg =>
          msg.message.passThrough.commitScaladsl()
        }
        .runWith(sink)

    terminateWhenDone(done)
  }
}
*/

/*
// Consume messages at-most-once
object AtMostOnceAkka extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #atMostOnce
    val rocket = new Rocket

    val done = Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
      .mapAsync(1) { record =>
        rocket.launch(record.value)
      }
      .runWith(Sink.ignore)
    // #atMostOnce

    terminateWhenDone(done)
  }
}

// Consume messages at-least-once
object AtLeastOnceAkka extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #atLeastOnce
    val db = new DB

    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(1) { msg =>
          db.update(msg.record.value).map(_ => msg)
        }
        .mapAsync(1) { msg =>
          msg.committableOffset.commitScaladsl()
        }
        .runWith(Sink.ignore)
    // #atLeastOnce

    terminateWhenDone(done)
  }
}

// Consume messages at-least-once, and commit in batches
object AtLeastOnceWithBatchCommitAkka extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #atLeastOnceBatch
    val db = new DB

    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(1) { msg =>
          db.update(msg.record.value).map(_ => msg.committableOffset)
        }
        .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #atLeastOnceBatch

    terminateWhenDone(done)
  }
}

// Connect a Consumer to Producer
object AkkaConsumerToProducerSink extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #consumerToProducerSink
    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map { msg =>
        println(s"topic1 -> topic2: $msg")
        ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
          "topic2",
          msg.record.value
        ), msg.committableOffset)
      }
      .runWith(Producer.commitableSink(producerSettings))
    // #consumerToProducerSink
  }
}




// Connect a Consumer to Producer, and commit in batches
object AkkaConsumerToProducerWithBatchCommits extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #consumerToProducerFlowBatch
    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map(msg =>
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.record.value), msg.committableOffset))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
    // #consumerToProducerFlowBatch

    terminateWhenDone(done)
  }
}

// Connect a Consumer to Producer, and commit in batches
object AkkaConsumerToProducerWithBatchCommits2 extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .map(msg =>
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.record.value), msg.committableOffset))
        .via(Producer.flow(producerSettings))
        .map(_.message.passThrough)
        // #groupedWithin
        .groupedWithin(10, 5.seconds)
        .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
        .mapAsync(3)(_.commitScaladsl())
        // #groupedWithin
        .runWith(Sink.ignore)

    terminateWhenDone(done)
  }
}

// Backpressure per partition with batch commit
object AkkaConsumerWithPerPartitionBackpressure extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource
    val done = Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .flatMapMerge(maxPartitions, _._2)
      .via(business)
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) =>
        batch.updated(elem.committableOffset)
      }
      .mapAsync(3)(_.commitScaladsl())
      .runWith(Sink.ignore)
    // #committablePartitionedSource

    terminateWhenDone(done)
  }
}

// Flow per partition
object AkkaConsumerWithIndependentFlowsPerPartition extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource2
    //Consumer group represented as Source[(TopicPartition, Source[Messages])]
    val consumerGroup =
      Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
    //Process each assigned partition separately
    consumerGroup.map {
      case (topicPartition, source) =>
        source
          .via(business)
          .toMat(Sink.ignore)(Keep.both)
          .run()
    }
      .mapAsyncUnordered(maxPartitions)(_._2)
      .runWith(Sink.ignore)
    // #committablePartitionedSource2
  }
}

// Join flows based on automatically assigned partitions
object AkkaConsumerWithOtherSource extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource3
    type Msg = CommittableMessage[Array[Byte], String]
    def zipper(left: Source[Msg, _], right: Source[Msg, _]): Source[(Msg, Msg), NotUsed] = ???

    Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .map {
        case (topicPartition, source) =>
          // get corresponding partition from other topic
          val otherSource = {
            val otherTopicPartition = new TopicPartition("otherTopic", topicPartition.partition())
            Consumer.committableSource(consumerSettings, Subscriptions.assignment(otherTopicPartition))
          }
          zipper(source, otherSource)
      }
      .flatMapMerge(maxPartitions, identity)
      .via(business)
      //build commit offsets
      .batch(max = 20, {
      case (l, r) => (
        CommittableOffsetBatch.empty.updated(l.committableOffset),
        CommittableOffsetBatch.empty.updated(r.committableOffset)
        )
    }) {
      case ((batchL, batchR), (l, r)) =>
        batchL.updated(l.committableOffset)
        batchR.updated(r.committableOffset)
        (batchL, batchR)
    }
      .mapAsync(1) { case (l, r) => l.commitScaladsl().map(_ => r) }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)
    // #committablePartitionedSource3
  }
}

//externally controlled kafka consumer
object ExternallyControlledKafkaAkkaConsumer extends AkkaConsumer {
  def main(args: Array[String]): Unit = {
    // #consumerActor
    //Consumer is represented by actor
    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    //Manually assign topic partition to it
    Consumer
      .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 1)))
      .via(business)
      .runWith(Sink.ignore)

    //Manually assign another topic partition
    Consumer
      .plainExternalSource[Array[Byte], String](consumer, Subscriptions.assignment(new TopicPartition("topic1", 2)))
      .via(business)
      .runWith(Sink.ignore)
    // #consumerActor
  }
}
*/
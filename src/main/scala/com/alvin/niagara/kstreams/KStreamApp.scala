package com.alvin.niagara.kstreams

import java.util.Properties

import com.alvin.niagara.common.Setting
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
/**
  * Created by alvinjin on 2017-02-06.
  */
object KStreamApp extends App with Setting {

  val builder: KStreamBuilder = new KStreamBuilder

  import StreamsConfig._

  val settings = new Properties()
  settings.put(APPLICATION_ID_CONFIG, "Kstream App")
  settings.put(BOOTSTRAP_SERVERS_CONFIG, brokerList)
  settings.put(ZOOKEEPER_CONNECT_CONFIG, zookeeperHost)
  settings.put(KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
  settings.put(VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)


  val stringSerde: Serde[String] = Serdes.String()

  import KeyValueImplicits._

  val textLines: KStream[Array[Byte], String] = builder.stream(textlineTopic)

  val uppercasedValues: KStream[String, String] = textLines.map((key, value) => (value, value.toUpperCase()))

  uppercasedValues.to(Serdes.String, Serdes.String, uppercaseTopic)

  val stream: KafkaStreams = new KafkaStreams(builder, settings)

  stream.start()

}

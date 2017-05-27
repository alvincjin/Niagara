package com.alvin.niagara.schemaregistry

import java.util

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

/**
  * A Scala variant of the specific Avro Serde shown in
  *
  * https://github.com/confluentinc/examples/blob/kafka-0.10.0.1-cp-3.0.1/kafka-streams/src/main/java/io/confluent/examples/streams/utils/SpecificAvroSerde.java
  *
  * @param registryProvider a function that returns a registry client and the corresponding url
  * @param properties optional properties for the serializer / deserializer
  * @tparam T the type to serialize / deserialize into
  */
class SpecificAvroSerde[T <: SpecificRecord](registryProvider: => (SchemaRegistryClient, String),
                                                  properties: util.Map[String, _] = new util.HashMap[String, Object]()) extends Serde[T] {
  val (registryClient, registryUrl) = registryProvider

  val innerSerde: Serde[T] = Serdes.serdeFrom[T](
    new SpecificAvroKafkaSerializer(Some(registryClient), properties),
    new SpecificAvroKafkaDeserializer(Some(registryClient), properties)
  )

  def withSpecificAvroReaderConfig(map: util.Map[String, _]) = {
    val configWithSpecificAvroReaderSet = new util.HashMap[String, Any]()
    configWithSpecificAvroReaderSet.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
    configWithSpecificAvroReaderSet.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl)
    configWithSpecificAvroReaderSet
  }

  class SpecificAvroKafkaSerializer(registryClient: Option[SchemaRegistryClient] = None,
                                    properties: util.Map[String, _] = new util.HashMap[String, Object]()) extends Serializer[T] {
    private val innerSerializer = registryClient.map(new KafkaAvroSerializer(_, withSpecificAvroReaderConfig(properties))).getOrElse(new KafkaAvroSerializer)

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = innerSerializer.configure(withSpecificAvroReaderConfig(configs), isKey)

    override def serialize(topic: String, record: T): Array[Byte] = innerSerializer.serialize(topic, record)

    override def close(): Unit = innerSerializer.close()
  }

  class SpecificAvroKafkaDeserializer(registryClient: Option[SchemaRegistryClient] = None,
                                      properties: util.Map[String, _] = new util.HashMap[String, Object]()  ) extends Deserializer[T] {
    private val innerDeserializer = registryClient.map(new KafkaAvroDeserializer(_, withSpecificAvroReaderConfig(properties))).getOrElse(new KafkaAvroDeserializer)

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = innerDeserializer.configure(withSpecificAvroReaderConfig(configs), isKey)

    override def deserialize(topic: String, record: Array[Byte]): T = innerDeserializer.deserialize(topic, record).asInstanceOf[T]

    override def close(): Unit = innerDeserializer.close()
  }

  override def deserializer(): Deserializer[T] = innerSerde.deserializer()

  override def serializer(): Serializer[T] = innerSerde.serializer()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = innerSerde.configure(configs, isKey)

  override def close(): Unit = innerSerde.close()
}
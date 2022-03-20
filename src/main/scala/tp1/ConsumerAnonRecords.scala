package tp1

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.{IterableHasAsJava, IterableHasAsScala}

class ConsumerAnonRecords {
  val props:Properties = new Properties()
  props.put("group.id", "group")
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("acks", "all")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(List[String]("anonymousRecords").asJavaCollection)

  def launchConsumer(): Unit = {
    try {
      while(true){
        val records = consumer.poll(Duration.ofMillis(1000))
        for(record <- records.asScala){
          if (record.value().contains("Malaise")) {
            println(record.value())
          }
        }
      }
      consumer.commitAsync()
    } finally {
      consumer.close()
    }
  }
}

object ConsumerAnonRecordsMain extends App {
  val kafkaConsumerClass = new ConsumerAnonRecords()
  kafkaConsumerClass.launchConsumer()
}


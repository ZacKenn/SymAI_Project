package tp1

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import java.util.Properties
import java.time.Duration
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}

class KafkaConsumerClass{
  val props:Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "sqdsqhdgg")


  val consumer = new KafkaConsumer(props)

  val tp1 = new TopicPartition("topic0",1)
  val topics = List[TopicPartition](tp1)
  consumer.subscribe(List[String]("topic0").asJava)

  def launchConsumer(): Unit = {
    try {
      while(true){
        val records = consumer.poll(Duration.ofMillis(1000))
        for(record <- records.asScala){
          println("Key: " + record.key() + ", Value: "+record.value() + ", Offser: " + record.offset())
        }
      }
      consumer.commitAsync()
    } finally {
      consumer.close()
    }
  }
}

object Main2 extends App {
  val kafkaConsumerClass = new KafkaConsumerClass()
  kafkaConsumerClass.launchConsumer()

}

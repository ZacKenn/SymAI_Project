package tp1

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Properties
import java.time.Duration
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord

class KafkaAvroConsumerClass {
  val props:Properties = new Properties()
  props.put("group.id", "group")
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.put("acks", "all")

  val consumer = new KafkaConsumer[String, Array[Byte]](props)

//  val tp1 = new TopicPartition("topic0",1)
//  val topics = List[TopicPartition](tp1)
  consumer.subscribe(List[String]("topic0").asJava)

  val schema = SchemaBuilder.record("person").fields()
    .requiredString("date")
    .requiredLong("id")
    .requiredString("firstName")
    .requiredString("lastName")
    .requiredString("vaccineName")
    .requiredString("sideEffect")
    .requiredString("siderCode")
    .endRecord()
  val test = GenericAvroCodecs.apply[GenericRecord](schema)

  def launchConsumer(): Unit = {
    try {
      while(true){
        val records = consumer.poll(Duration.ofMillis(1000))
        for(record <- records.asScala){
          println("Key: " + record.key() + ", Value: "+ test.invert(record.value()) + ", Offset: " + record.offset())
        }
      }
      consumer.commitAsync()
    } finally {
      consumer.close()
    }
  }

}
object Main2 extends App {
  val kafkaConsumerClass = new KafkaAvroConsumerClass()
  kafkaConsumerClass.launchConsumer()
}

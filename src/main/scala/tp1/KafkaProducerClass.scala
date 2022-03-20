package tp1

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.{JsValue, Json}

import java.util.Properties

class KafkaProducerClass {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")

  val producer = new KafkaProducer[String, String](props)
  val topic = "topic2"

  def sendRecord(record : JsValue): Unit = {
    val res = new ProducerRecord[String, String](topic,(record \ "lastName").get.as[String],record.toString())
    producer.send(res)
  }
}

package tp1

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Properties
import java.time.Duration
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.{JsObject, Json}

class SideEffectConsumer {
  val props:Properties = new Properties()
  props.put("group.id", "group"+System.currentTimeMillis())
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("acks", "all")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List[String]("anonymousSideEffect").asJava)

  def launchConsumer(): Unit = {
    try {
      println("Start consumer")
      while(true){
        val records = consumer.poll(Duration.ofMillis(1000))
        val sideEffectRecords = records.asScala.filter(record => {
          val person = Json.parse(record.value())
          person("siderCode").as[String].equals("C0027497")
        })
        for(record <- sideEffectRecords){
          val person = Json.parse(record.value())
          println("Key: " + record.key() + ", Value: " + person + ", Offset: " + record.offset())
        }
      }
      consumer.commitAsync()
    } finally {
      consumer.close()
    }
  }

}
object SideEffectConsumerMain extends App {
  val sideEffectConsumer = new SideEffectConsumer()
  sideEffectConsumer.launchConsumer()
}

package tp1

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

class KafkaProducerClass{
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")

  val producer = new KafkaProducer[String, String](props)
  val topic = "topic0"

  def launchProducer(): Unit = {
    try {
      for (i <- 0 to 10) {
        val record = new ProducerRecord[String, String](topic, i.toString, i.toString)
        producer.send(record)
      }
    } finally {
      producer.close()
    }
  }
}

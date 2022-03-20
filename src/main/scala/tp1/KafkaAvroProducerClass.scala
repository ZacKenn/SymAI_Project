package tp1

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

class KafkaAvroProducerClass{
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("acks", "all")

  val producer = new KafkaProducer[String, Array[Byte]](props)
  val topic = "topic0"

  def produceRecord(schema : Schema, date : String, id : Long, firstName : String, lastName : String,
                    vaccineName : String, sideEffect : String, siderCode : String): GenericData.Record = {
    val tmp = new GenericData.Record(schema)
    tmp.put("date", date)
    tmp.put("id", id)
    tmp.put("firstName", firstName)
    tmp.put("lastName", lastName)
    tmp.put("vaccineName", vaccineName)
    tmp.put("sideEffect", sideEffect)
    tmp.put("siderCode", siderCode)
    tmp
  }

  def sendRecord(schema : Schema, record : GenericData.Record): Unit = {
    val test = GenericAvroCodecs.apply[GenericRecord](schema)
    val personDataToSend = test.apply(record)
    val name : String = record.get("firstName").toString
    try{
      val record = new ProducerRecord[String, Array[Byte]](topic, name, personDataToSend)
      producer.send(record)
    }
  }


}

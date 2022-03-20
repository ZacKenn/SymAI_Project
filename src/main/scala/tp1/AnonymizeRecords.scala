package tp1

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Consumed, KStream, KTable, Produced}
import play.api.libs.json.Json

import java.util
import java.util.regex.Pattern
import java.util.Properties
import scala.reflect.ClassTag

class AnonymizeRecords extends Runnable{
  override def run(): Unit = {
    val props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anonymize-record");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
      Serdes.String().getClass().getName())
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      Serdes.String().getClass().getName())

    val streamingConfig = new StreamsConfig(props)
    val builder = new StreamsBuilder

    val source: KStream[String, String] = builder.stream("topic2",Consumed.`with`(Serdes.String(), Serdes.String()))

    val counts = source.
      map((key, value) => {
        val person = Json.parse(value)
        val tmp = List(person("gender").toString,person("zipcode").toString(),
                       person("birthDate").toString(),person("vaccinationDate").toString(),
          person("vaccineName").toString(),person("sideEffect").toString())
        new KeyValue[String, String](key, Json.toJson(tmp).toString())
        })


    counts.foreach((k, v) => {System.out.println(k + " " + v)
    })
    counts.to("anonymousRecords")
    val streams = new KafkaStreams(builder.build, props)
    System.out.println("start")
    streams.start()
    Thread.sleep(3000L)
    streams.close()
    streams.cleanUp()
  }
}

object AnonymizeRecordsMain extends App {
  val anonymizeRecords = new AnonymizeRecords
  anonymizeRecords.run()
}

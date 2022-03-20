package tp1

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json.{Json, Writes}
import java.util.Properties

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

    implicit val anonymousPersonWrites = new Writes[AnonymousPerson] {
      def writes(anonymousPerson: AnonymousPerson) = Json.obj(
        "id" -> anonymousPerson.id,
        "gender"  -> anonymousPerson.gender,
        "zipcode"-> anonymousPerson.zipcode,
        "birthDate" -> anonymousPerson.birthDate,
        "vaccinationDate" -> anonymousPerson.vaccinationDate,
        "vaccineName" -> anonymousPerson.vaccineName,
        "sideEffect" -> anonymousPerson.sideEffect,
        "siderCode" -> anonymousPerson.siderCode
      )
    }

    def convertAnonymousPersonToJSON(anonymousPerson: AnonymousPerson) = {
      val obj = Json.toJson(anonymousPerson)
      obj
    }

    val anonymizeRecordsResult = source.
      map((key, value) => {
        val person = Json.parse(value)
        val tmp = new AnonymousPerson(person("id").toString().toLong, person("gender").as[String],person("zipcode").as[String],
          person("birthDate").toString(),person("vaccinationDate").toString(),
          person("vaccineName").as[String],person("sideEffect").as[String], person("siderCode").as[String])
        new KeyValue[String, String](key, convertAnonymousPersonToJSON(tmp).toString())
      })


    anonymizeRecordsResult.foreach((k, v) => {System.out.println(k + " " + v)
    })
    anonymizeRecordsResult.to("anonymousSideEffect")
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

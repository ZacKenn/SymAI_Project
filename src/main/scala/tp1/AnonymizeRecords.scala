package tp1

import org.apache.jena.ext.com.google.common.collect.ImmutableList
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Consumed, KStream, Predicate, Produced}
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json.{JsObject, Json, Writes}

import java.util.{Date, Properties}

class AnonymizeRecords extends Runnable{
  override def run(): Unit = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anonymize-record")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put("max.poll.records", 50)
    props.put("auto.offset.reset", "latest")
    props.put("session.timeout.ms", 6000)
    props.put("group.initial.rebalance.delay.ms", 0)
    props.put("max.poll.interval.ms", 6000)
    props.put("heartbeat.interval.ms", 1000)

    val adminClient: AdminClient    = AdminClient.create(props)
    val topic : NewTopic  = new NewTopic("newTopic",5, 1:(Short))
    adminClient.createTopics(ImmutableList.of(topic))

    val builder = new StreamsBuilder

    val source: KStream[String, String] = builder.stream("topic2",Consumed.`with`(Serdes.String(), Serdes.String()))

    implicit val anonymousPersonWrites: Writes[AnonymousPerson] = new Writes[AnonymousPerson] {
      def writes(anonymousPerson: AnonymousPerson): JsObject = Json.obj(
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

    // QUESTION 2

    val anonymizeRecordsResult = source.
      map((key, value) => {
        val person = Json.parse(value)
        val tmp = new AnonymousPerson(person("id").toString().toLong, person("gender").as[String],person("zipcode").as[String],
          person("birthDate").as[Date],person("vaccinationDate").as[Date],
          person("vaccineName").as[String],person("sideEffect").as[String], person("siderCode").as[String])
        new KeyValue[String, String](key, convertAnonymousPersonToJSON(tmp).toString())
      })


    // QUESTION 4

    val countSideEffects = source.map((key, value) => {
      val person = Json.parse(value)
      new KeyValue[String, String](person("sideEffect").as[String], person("sideEffect").as[String])
    }).groupByKey().count().toStream()
//    countSideEffects.foreach((k, v) => {System.out.println(k + " " + v)})

    // Question 5
        val isPfizer : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("Pfizer")
        val isModerna : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("Moderna")
        val isAstraZeneca : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("AstraZeneca")
        val isSpoutnikV : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("SpoutnikV")
        val isCanSinoBio : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("CanSinoBio")
        val kstreamVaccin : Array[KStream[String, String]] = source.branch(isPfizer,isModerna,isAstraZeneca,isSpoutnikV,isCanSinoBio)

        kstreamVaccin.foreach( elem =>{
          val tmp = elem.map((key, value) => {
            val person = Json.parse(value)
            new KeyValue[String, String](person("vaccineName").as[String] + " : " + person("sideEffect").as[String], person("sideEffect").as[String])
          }).groupByKey().count().toStream()
          tmp.to("newTopic")
        })
    

    countSideEffects.to("numberOfSideEffect")

//    anonymizeRecordsResult.foreach((k, v) => {System.out.println(k + " " + v)})
    anonymizeRecordsResult.to("anonymousSideEffect",Produced.`with`(Serdes.String,Serdes.String))
    val streams = new KafkaStreams(builder.build, props)
    System.out.println("start")
    streams.start()
    Thread.sleep(10000L)
    streams.close()
    streams.cleanUp()
  }
}

object AnonymizeRecordsMain extends App {
  val anonymizeRecords = new AnonymizeRecords
  anonymizeRecords.run()
}

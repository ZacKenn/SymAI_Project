//package tp1
//
//import org.apache.jena.ext.com.google.common.collect.ImmutableList
//import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
//import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}
//import org.apache.kafka.streams.kstream.{Consumed, KStream, Predicate}
//import play.api.libs.json.Json
//
//import java.util.{Date, Properties}
//
//class ConsumerGroupSideEffect extends Runnable{
//  override def run(): Unit = {
//    val props = new Properties();
//    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "side-effect");
//    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
//    props.put("acks", "all")
//    props.put("max.poll.records", 50);
//    props.put("auto.offset.reset", "latest");
//    props.put("session.timeout.ms", 6000);
//    props.put("group.initial.rebalance.delay.ms", 0);
//    props.put("max.poll.interval.ms", 6000);
//    props.put("heartbeat.interval.ms", 1000)
//
//    val adminClient: AdminClient    = AdminClient.create(props)
//    val topic : NewTopic  = new NewTopic("newTopic",5, 1:(Short))
//    adminClient.createTopics(ImmutableList.of(topic))
//
//    val builder = new StreamsBuilder
//    val source: KStream[String, String] = builder.stream("anonymousSideEffect",Consumed.`with`(Serdes.String(), Serdes.String()))
//    source.foreach((k,v) => println(k + " " + v))
//    val isPfizer : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("Pfizer")
//    val isModerna : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("Moderna")
//    val isAstraZeneca : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("AstraZeneca")
//    val isSpoutnikV : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("SpoutnikV")
//    val isCanSinoBio : Predicate[String, String]  = (key, value) => Json.parse(value)("vaccineName").as[String].equalsIgnoreCase("CanSinoBio")
//    val kstreamByDept : Array[KStream[String, String]] = source.branch(isPfizer,isModerna,isAstraZeneca,isSpoutnikV,isCanSinoBio)
//
//    kstreamByDept.foreach( elem =>{
//      var name : String = ""
//      val tmp = elem.map((key, value) => {
//        val person = Json.parse(value)
//        name = person("vaccineName").as[String]
//        new KeyValue[String, String](person("sideEffect").as[String], person("sideEffect").as[String])
//      }).groupByKey().count().toStream()
//      tmp.to("Moderna")
//    })
//
//    val streams = new KafkaStreams(builder.build, props)
//    System.out.println("start")
//    streams.start()
//    Thread.sleep(10000L)
//    streams.close()
//    streams.cleanUp()
//  }
//}
//
//object ConsumerGroupSideEffectMain extends App {
//  val consum = new ConsumerGroupSideEffect
//  consum.run()
//}

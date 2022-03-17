package tp1

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}

import java.util
import java.util.regex.Pattern
import java.util.Properties

class AnonymizeRecords extends Runnable{
  override def run(): Unit = {
    val props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anonymize-record");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
      Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      Serdes.String().getClass().getName());

    val streamingConfig = new StreamsConfig(props)
    val builder = new StreamsBuilder
    val source: KStream[String, String] = builder.stream("topic0")

    val pattern: Pattern = Pattern.compile("\\W+")

    val counts = source.
      map((key, value) => new KeyValue[String, String](value, value))
      .groupByKey.count()
      .toStream

    counts.foreach((k, v) => System.out.println(k + " " + v))
    counts.to("anonymousRecords")

    val streams = new KafkaStreams(builder.build, props)
    System.out.println("start")
    streams.start()
    Thread.sleep(30000L)
    streams.close()
  }
}

object AnonymizeRecordsMain extends App {
  val anonymizeRecords = new AnonymizeRecords
  anonymizeRecords.run()
}

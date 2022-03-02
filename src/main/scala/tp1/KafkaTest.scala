package tp1

import com.github.javafaker.{DateAndTime, Faker}
import play.api.libs.json.Json

import java.util.{Date, Locale}
import scala.collection.mutable.ListBuffer
import org.apache.kafka.clients.producer.KafkaProducer

class Person(val lastName: String, val firstName: String, val birthDate: Date, val state: String) {

}


class KafkaTest {
  val f = new Faker(new Locale("us"));
  val persons = new ListBuffer[Person]


  def generateRecords():Unit = {
    for (i <- 0 to 10) {
      val p = new Person(f.name().lastName(), f.name().firstName(), f.date().birthday(), f.address().state())
      persons += p
    }
  }

  def convertToJSON(): Unit = {
    for (p <- persons) {
      val jsonObject = Json.toJson(p.lastName, p.firstName, p.birthDate, p.state)
      println(jsonObject)
    }
  }

}

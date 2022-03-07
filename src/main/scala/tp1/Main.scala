package tp1

import com.github.javafaker.Faker
import com.github.javafaker.service.{FakeValuesService, RandomService}

import java.util.Locale

object Main extends App {
  val test = new Test("file:lubm1.ttl")
  test.load("file:lubm1.ttl")
  test.addOntology()
  test.topicForVaccinatedPersons("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")

//  val test = new KafkaTest()
//  test.generateRecords()
//  val persons = test.persons
//  test.convertToJSON()
//  val kafkaProducerClass = new KafkaProducerClass()
//  kafkaProducerClass.launchProducer()


}

package tp1

import com.github.javafaker.Faker
import com.github.javafaker.service.{FakeValuesService, RandomService}

import java.util.Locale

object Main extends App {
  val test = new Test("file:lubm1.ttl")
  test.load()

  test.addOntology()
//  test.topicForVaccinatedPersons("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")

}

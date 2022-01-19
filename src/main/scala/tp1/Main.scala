package tp1

import com.github.javafaker.Faker
import com.github.javafaker.service.{FakeValuesService, RandomService}

import java.util.Locale

object Main extends App {
  val test = new Test("file:///home/lim/M2 S1/SymAI_Project_Scala/lubm1.ttl")

  val f = new Faker()
//  val fvs = new FakeValuesService(
//    new Locale("en-GB"), new RandomService())




  test.load()
  test.showModel()
  println(test.size())
  test.generateRDF()
}

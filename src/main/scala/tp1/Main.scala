package tp1

object Main extends App {
  val test = new Test("file:lubm1.ttl")
  test.load()

  test.addOntology()
  Thread.sleep(5000L)
  val anonymizeRecords = new AnonymizeRecords
  anonymizeRecords.run()
//  test.topicForVaccinatedPersons("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")

}

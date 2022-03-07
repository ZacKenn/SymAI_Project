package tp1

import com.github.javafaker.Faker
import org.apache.jena.ontology.OntModelSpec
import org.apache.jena.rdf.model._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.{JsValue, Json}

import java.io.{File, PrintWriter}
import java.time.Instant
import java.util.{Date, Locale, Properties}
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer

class Person(val lastName: String, val firstName: String, val gender: String, val zipcode: String, val birthDate: Date, val vaccinationDate: Date,
             val vaccineName: String, val sideEffect: String) {

}

class Test(val dbSource : String) {
  val model = ModelFactory.createDefaultModel();
  val identifierRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#identifier")
  val firstNameRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#firstName")
  val lastNameRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#lastName")
  val genderRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#gender")
  val zipcodeRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode")
  val dateOfBirthRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#dateOfBirth")
  val dateOfVaccination = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#dateOfVaccination")
  val vaccineName = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#vaccineName")
  val sideEffectRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#sideEffect")
  val f = new Faker(new Locale("us"));
  val vaccine = List( "Pfizer", "Moderna", "AstraZeneca", "SpoutnikV", "CanSinoBio")

  val sideEffects = List("C0151828","C0015672")

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")

  val producer = new KafkaProducer[String, String](props)
  val topic = "topic0"

  def load(dbSource : String) = model.read(dbSource, "TTL")
  def showModel() : Unit = println("is empty ? "  + model.isEmpty())
  def size() : Long = model.size()

  def getDistinctSubjects() = {
    val it = model.listStatements()
    val props = new ListBuffer[Resource]
    while (it.hasNext) props += it.next().getSubject

    props.toList.distinct
  }

  def addStatementForClass(subclass : String, printWriter: PrintWriter) : Unit = {
    val typeProperty = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    val rdfType = model.createProperty(typeProperty)
    val person = model.createResource(subclass)
    val it = model.listSubjectsWithProperty(rdfType, person)

    val personExtension = new ListBuffer[Statement]

    it.toList.distinct.foreach(x => {
      val sideEff = sideEffects(f.number().numberBetween(0,1))
      val persObj = new Person(f.name().lastName(), f.name().firstName(),
        f.regexify("[FM]{1}"),
        f.address().zipCode().toString(),
        f.date().birthday(30,71),
        f.date().birthday(0,3),
        vaccine(f.number().numberBetween(0,5)),
        sideEffects(f.number().numberBetween(0, 0))
      )
      personExtension += model.createStatement(x,identifierRDF,model.createResource(f.number().randomNumber().toString()))
      personExtension += model.createStatement(x,firstNameRDF,model.createResource(persObj.firstName))
      personExtension += model.createStatement(x,lastNameRDF,model.createResource(persObj.lastName))
      personExtension += model.createStatement(x,genderRDF,model.createResource(persObj.gender))
      personExtension += model.createStatement(x,zipcodeRDF,model.createResource(persObj.zipcode))
      personExtension += model.createStatement(x,dateOfBirthRDF,model.createResource(persObj.birthDate.toString))
      personExtension += model.createStatement(x,dateOfVaccination,model.createResource(persObj.vaccinationDate.toString))
      personExtension += model.createStatement(x,vaccineName,model.createResource(persObj.vaccineName))
      personExtension += model.createStatement(x,sideEffectRDF,model.createResource(sideEff))

      try{
        val record = new ProducerRecord[String, String](topic, persObj.lastName, convertToJSON(persObj).toString())
        producer.send(record)
      }
    })

    personExtension.foreach(x => printWriter.write("<" + x.getSubject + "> <" + x.getPredicate + "> \"" + x.getResource + "\" .\n"))
  }

  def addOntology(): Unit = {
    val inf = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MINI_RULE_INF)
    inf.read("file:src/main/resources/univ-bench.owl")
    val pers = inf.getOntClass("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")
    val ext = new File("lubm1PersonExtension.ttl")
    val printWriter = new PrintWriter(ext)
    pers.listSubClasses(false).filterDrop(c => c.getURI==null).toList.forEach(x => {
      addStatementForClass(x.getURI, printWriter)
    })
    producer.close()
    printWriter.close()
  }

  def convertToJSON(person: Person): JsValue = {
    val jsonObject = Json.toJson(person.lastName, person.firstName, person.gender,
      person.zipcode, person.birthDate, person.vaccinationDate, person.vaccineName, person.sideEffect)
    jsonObject
  }

}

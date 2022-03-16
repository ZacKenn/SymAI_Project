package tp1

import com.github.javafaker.Faker
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.jena.ontology.OntModelSpec
import org.apache.jena.rdf.model._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.{JsValue, Json}

import java.io.{File, PrintWriter}
import java.util.{Date, Locale, Properties}
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

class Person(val id : Long, val lastName: String, val firstName: String, val gender: String, val zipcode: String, val birthDate: Date, val vaccinationDate: Date,
             val vaccineName: String, val sideEffect: String, val siderCode : String) {

}

class PersonAvro(val date: Date, val id: Long, val firstName: String, val lastName: String, val vaccineName: String, val sideEffect: String,
                 val siderCode: String) {
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
  val siderCodeRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#siderCode")
  val f = new Faker(new Locale("us"));
  val vaccine = List( "Pfizer", "Moderna", "AstraZeneca", "SpoutnikV", "CanSinoBio")

  val sideEffects = HashMap("C0151828"->"Injection site pain", "C0015672"->"fatigue", "C0018681"->"headache", "C0231528"->"Muscle pain",
  "C0085593"->"chills", "C0003862"->"Joint pain", "C0015967"->"fever", "C0151605"->"Injection site swelling", "C0852625"->"Injection site redness",
  "C0027497"->"Nausea", "C0231218"->"Malaise", "C0497156"->"Lymphadenopathy", "C0863083"->"Injection site tenderness")

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put("acks", "all")

  val producer = new KafkaProducer[String, Array[Byte]](props)
  val topic = "topic0"

  def load() = model.read(dbSource, "TTL")
  def showModel() : Unit = println("is empty ? "  + model.isEmpty())
  def size() : Long = model.size()

  def getDistinctSubjects() = {
    val it = model.listStatements()
    val props = new ListBuffer[Resource]
    while (it.hasNext) props += it.next().getSubject

    props.toList.distinct
  }

  def avroSchemaInitializer() : Schema = {
    val schema = SchemaBuilder.record("person").fields()
      .requiredString("date")
      .requiredLong("id")
      .requiredString("firstName")
      .requiredString("lastName")
      .requiredString("vaccineName")
      .requiredString("sideEffect")
      .requiredString("siderCode")
      .endRecord()
    schema
  }

  def produceRecord(schema : Schema, date : String, id : Long, firstName : String, lastName : String,
                    vaccineName : String, sideEffect : String, siderCode : String): GenericData.Record = {
    val tmp = new GenericData.Record(schema)
    tmp.put("date", date)
    tmp.put("id", id)
    tmp.put("firstName", firstName)
    tmp.put("lastName", lastName)
    tmp.put("vaccineName", vaccineName)
    tmp.put("sideEffect", sideEffect)
    tmp.put("siderCode", siderCode)
    tmp
  }

  def sendRecord(schema : Schema, record : GenericData.Record): Unit = {
    val test = GenericAvroCodecs.apply[GenericRecord](schema)
    val personDataToSend = test.apply(record)
    try{
      val record = new ProducerRecord[String, Array[Byte]](topic, "test", personDataToSend)
      producer.send(record)
    }
  }

  def addStatementForClass(subclass : String, printWriter: PrintWriter, schema : Schema) : Unit = {
    val typeProperty = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    val rdfType = model.createProperty(typeProperty)
    val person = model.createResource(subclass)
    val it = model.listSubjectsWithProperty(rdfType, person)

    val personExtension = new ListBuffer[Statement]

    it.toList.distinct.foreach(x => {
      val sideEff = sideEffects.toList(f.number().numberBetween(0,sideEffects.size))
      val id = f.number().randomNumber()
      val persObj = new Person(
        id,
        f.name().lastName(), f.name().firstName(),
        f.regexify("[FM]{1}"),
        f.address().zipCode(),
        f.date().birthday(30,71),
        f.date().birthday(0,3),
        vaccine(f.number().numberBetween(0,5)),
        sideEff._2,
        sideEff._1
      )
      personExtension += model.createStatement(x,identifierRDF,model.createResource(persObj.id.toString))
      personExtension += model.createStatement(x,firstNameRDF,model.createResource(persObj.firstName))
      personExtension += model.createStatement(x,lastNameRDF,model.createResource(persObj.lastName))
      personExtension += model.createStatement(x,genderRDF,model.createResource(persObj.gender))
      personExtension += model.createStatement(x,zipcodeRDF,model.createResource(persObj.zipcode))
      personExtension += model.createStatement(x,dateOfBirthRDF,model.createResource(persObj.birthDate.toString))
      personExtension += model.createStatement(x,dateOfVaccination,model.createResource(persObj.vaccinationDate.toString))
      personExtension += model.createStatement(x,vaccineName,model.createResource(persObj.vaccineName))
      personExtension += model.createStatement(x,sideEffectRDF,model.createResource(persObj.sideEffect))
      personExtension += model.createStatement(x,siderCodeRDF,model.createResource(persObj.siderCode))

      val record = produceRecord(schema, persObj.vaccinationDate.toString, persObj.id, persObj.firstName, persObj.lastName, persObj.vaccineName, persObj.sideEffect, persObj.siderCode)
      sendRecord(schema, record)
    })

    personExtension.foreach(x => printWriter.append("<" + x.getSubject + "> <" + x.getPredicate + "> \"" + x.getResource + "\" .\n"))
  }

  def addOntology(): Unit = {
    val inf = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MINI_RULE_INF)
    inf.read("file:src/main/resources/univ-bench.owl")
    val pers = inf.getOntClass("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")
    val ext = new File("lubm1extension.ttl")
    val printWriter = new PrintWriter(ext)
    val schema = avroSchemaInitializer()
    pers.listSubClasses(false).filterDrop(c => c.getURI==null).toList.forEach(x => {
      addStatementForClass(x.getURI, printWriter, schema)
    })
    producer.close()
    printWriter.close()
  }

  def convertToJSON(person: Person): JsValue = {
    val jsonObject = Json.toJson(person.lastName, person.firstName, person.gender,
      person.zipcode, person.birthDate, person.vaccinationDate, person.vaccineName, person.sideEffect)
    jsonObject
  }

//  def topicForVaccinatedPersons(subClass : String) : Unit = {
//    val typeProperty = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//    val rdfType = model.createProperty(typeProperty)
//    val person = model.createResource(subClass)
//    val vaccineProperty = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#vaccineName")
//    val it = model.listSubjectsWithProperty(rdfType, person)
//    model.listSubjectsWithProperty(vaccineProperty).forEach(println)
//  }
}

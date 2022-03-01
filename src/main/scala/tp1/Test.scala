package tp1

import com.github.javafaker.Faker
import org.apache.jena.ontology.OntModelSpec
import org.apache.jena.rdf.model._

import java.io.{File, PrintWriter}
import java.util.Locale
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer

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
  val f = new Faker(new Locale("us"));
  val vaccine = List( "Pfizer", "Moderna", "AstraZeneca", "SpoutnikV", "CanSinoBio")

  def load() = model.read(dbSource, "TTL")
  def showModel() : Unit = println("is empty ? "  + model.isEmpty())
  def size() : Long = model.size()

  def getDistinctSubjects() = {
    val it = model.listStatements()
    val props = new ListBuffer[Resource]
    while (it.hasNext) props += it.next().getSubject

    props.toList.distinct
  }

  def addStatement() : Unit = {
    val typeProperty = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    val rdfType = model.createProperty(typeProperty)
    val fullprofessor = model.createResource("http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor")
    val it = model.listSubjectsWithProperty(rdfType,fullprofessor)

    val ext = new File("lubm1extension.ttl")
    val printWriter = new PrintWriter(ext)
    val fullProfessorExtension = new ListBuffer[Statement]

    it.toList.distinct.foreach(x => {
      fullProfessorExtension += model.createStatement(x,identifierRDF,model.createResource(""+f.number().randomNumber()))
      fullProfessorExtension += model.createStatement(x,firstNameRDF,model.createResource(f.name().firstName()))
      fullProfessorExtension += model.createStatement(x,lastNameRDF,model.createResource(f.name().lastName()))
      fullProfessorExtension += model.createStatement(x,genderRDF,model.createResource(f.regexify("[FM]{1}")))
      fullProfessorExtension += model.createStatement(x,zipcodeRDF,model.createResource(""+f.address().zipCode()))
      fullProfessorExtension += model.createStatement(x,dateOfBirthRDF,model.createResource(""+f.date().birthday(30,71)))
      fullProfessorExtension += model.createStatement(x,dateOfVaccination,model.createResource(""+f.date().birthday(0,3)))
      fullProfessorExtension += model.createStatement(x,vaccineName,model.createResource(vaccine(f.number().numberBetween(0,5))))
    })

    fullProfessorExtension.foreach(x => printWriter.write("<" + x.getSubject + "> <" + x.getPredicate + "> \"" + x.getResource + "\" .\n"))
    printWriter.close()
  }

  def addStatementForClass(subclass : String, printWriter: PrintWriter) : Unit = {
    val typeProperty = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    val rdfType = model.createProperty(typeProperty)
    val person = model.createResource(subclass)
    val it = model.listSubjectsWithProperty(rdfType, person)

    val ext = new File("lubm1extension.ttl")
    val personExtension = new ListBuffer[Statement]

    it.toList.distinct.foreach(x => {
      personExtension += model.createStatement(x,identifierRDF,model.createResource(""+f.number().randomNumber()))
      personExtension += model.createStatement(x,firstNameRDF,model.createResource(f.name().firstName()))
      personExtension += model.createStatement(x,lastNameRDF,model.createResource(f.name().lastName()))
      personExtension += model.createStatement(x,genderRDF,model.createResource(f.regexify("[FM]{1}")))
      personExtension += model.createStatement(x,zipcodeRDF,model.createResource(""+f.address().zipCode()))
      personExtension += model.createStatement(x,dateOfBirthRDF,model.createResource(""+f.date().birthday(30,71)))
      personExtension += model.createStatement(x,dateOfVaccination,model.createResource(""+f.date().birthday(0,3)))
      personExtension += model.createStatement(x,vaccineName,model.createResource(vaccine(f.number().numberBetween(0,5))))
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
      println(x.getURI)
      addStatementForClass(x.getURI, printWriter)
    })
    printWriter.close()
  }



}

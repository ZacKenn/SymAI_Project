package tp1

import org.apache.jena.rdf.model._

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer

class Test(val dbSource : String) {
  val model = ModelFactory.createDefaultModel();

  def load() = model.read(dbSource, "TTL")
  def showModel() : Unit = println("is empty ? "  + model.isEmpty())
  def size() : Long = model.size()

  def getDistinctSubjects() = {
    val it = model.listStatements()
    val props = new ListBuffer[Resource]
    while (it.hasNext) props += it.next().getSubject

    props.toList.distinct
  }

  def generateRDF() = {
    val identifierRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#identifier")
    val firstNameRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#firstName")
    val lastNameRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#lastName")
    val genderRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#gender")
    val zipcodeRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode")
    val dateOfBirthRDF = model.createProperty("http://swat.cse.lehigh.edu/onto/univ-bench.owl#dateOfBirth")

//    getDistinctSubjects().foreach(x => println(x))
    val typeProperty = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"
    val rdfType = model.createProperty(typeProperty)
    val it = model.listSubjectsWithProperty(rdfType)
    it.toList.distinct.foreach(x => println(x))
  }



}

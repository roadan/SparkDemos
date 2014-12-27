import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

// creating a case class
case class Actor(id: String, name: String, moviesCount: Int)

/**
 * This demo shows the creation of SchemaRDDs from RDD[String] and JSON files
 * simple SQL execution and Scala UDFs
 */
class SparkSQL {

  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark SQL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.createSchemaRDD

    val actorsFile = sc.textFile("file:/Users/roadan/Work/Sessions/Spark/Demos/Data/actors")
    val actors = actorsFile.map(_.split(",")).map(a => Actor(id = a(0).drop(1), name = a(1), moviesCount = a(2).dropRight(1).toInt))

    actors.registerTempTable("actors")

    val topTierActors = sqlContext.sql("SELECT name,moviesCount FROM actors ORDER BY moviesCount DESC LIMIT 10")

    sqlContext.cacheTable("actors")

    // the movies RDD will have schema based on the underlying JSON structure
    // so we can simply register it as a table
    val marvelActors = sqlContext.jsonFile("file:/Users/roadan/Work/Sessions/Spark/Demos/Data/marvel_actors.json")
    marvelActors.registerTempTable("marvelActors")

    // let's create a UDF
    def stringify (a: ArrayBuffer[String]) = a.mkString(",")
    sqlContext.registerFunction("stringify", stringify _)
    val movisWithFlatCast = sqlContext.sql("SELECT name, stringify(titles) FROM marvelActors")

    //val hiveContext = new HiveContext(sc)

  }

}

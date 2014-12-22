import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

/**
 * this demo shows a simple aggregations over RDDs
 */
class Aggregations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Aggregations")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // creating the initial dataset
    // loading the movies from a JSON file
    val movies = sqlContext.jsonFile("file:/Users/roadan/Work/Sessions/Spark/Demos/Data/marvel_movies.json")

    movies.toDebugString
    // creating an RDD with tuples of (actor_id, movie_title)
    val sMovies = movies.map(m => (m(0).asInstanceOf[ArrayBuffer[String]],m(2)))
    val cast = sMovies.flatMap(m => {
      for(i<-0 to m._1.size-1) yield ( m._1(i).toString, m._2)
    })

    // loading the actors from a JSON file and creating tuples
    val actors = sqlContext.jsonFile("file:/Users/roadan/Work/Sessions/Spark/Demos/Data/marvel_actors.json")
    val actorsFormat = actors.map(a => (a(0).toString, a(1)))

    // Joining the datasets
    val actorsAndMovies = actorsFormat.join(cast).map(a => a._2)
    actorsAndMovies.foreach(println)

    // starting the aggregations demo
    // ******************************

    // creating a list of all the movies names
    val mNames = movies.map(m => (m(2).toString))
    val mNamesString = mNames.reduce(_ + ", " + _)

    // creating a list of all the movies names per actor
    val actorMovies = actorsAndMovies.reduceByKey(_ + ", " + _)
    actorMovies.foreach(println)

    // creating a list of all the actors and the value 1 for each movie
    val actorsCount = actorsAndMovies.map(a => (a._1.toString, 1))

    // this is not a proper use, since the movies ordered will be added
    // for each partition but hey, what the Marvel universe for you...
    val actorMoviesOrdered = actorsAndMovies.foldByKey(" movies ordered: ")(_ + ", " + _)
    actorMoviesOrdered.foreach(println)

  }

}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

/**
 * this demo shows a simple aggregations over RDDs
 */
object Aggregations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Aggregations")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.createSchemaRDD

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
    val mNames = movies.map(m => m(2).toString)
    val mNamesString = mNames.reduce(_ + ", " + _)

    // creating a list of all the movies names per actor
    val actorMovies = actorsAndMovies.reduceByKey(_ + ", " + _)
    actorMovies.foreach(println)

    // this is not a proper use, since the movies ordered will be added
    // for each partition but hey, what the Marvel universe for you...
    val actorMoviesOrdered = actorsAndMovies.foldByKey(" movies ordered: ")(_ + ", " + _)
    actorMoviesOrdered.foreach(println)

    // creating a list of all the actors and the value 1 for each movie
    val actorsCount = actorsAndMovies.map(a => (a._1.toString, 1)).reduceByKey(_ + _)
    actorsCount.foreach(println)

    val actorsMoviesStr = actorsAndMovies.map(a => (a._1.toString, a._2.toString))
    val moviesToCount = actorsMoviesStr.join(actorsCount).map(m => m._2)

    // calculating the average of movies per actor per movie
    val avgAccMoviesPerMovie = moviesToCount.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }

  }

}

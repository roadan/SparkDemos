import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

/**
* this demo shows a simple join between two RDDs as well as union
* cogroup and intersection
*/
class JoiningData {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Joining Data")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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

    // loading the movies from a JSON file
    val tvSeries = sqlContext.jsonFile("file:/Users/roadan/Work/Sessions/Spark/Demos/Data/marvel_shows.json")

    // creating an RDD with tuples of (actor_id, movie_title)
    val sTvSeries = tvSeries.map(m => (m(0).asInstanceOf[ArrayBuffer[String]],m(2)))
    val tvCast = sTvSeries.flatMap(m => {
      for(i<-0 to m._1.size-1) yield ( m._1(i).toString, m._2)
    })

    // Joining the datasets for tv shows
    val actorsAndShows = actorsFormat.join(tvCast).map(a => a._2)
    actorsAndShows.foreach(println)

    // getting only cast members that appeared in both the movies and tv
    // versions of the marvel universe
    val tvAndMoviesCast = actorsAndMovies.intersection(actorsAndShows)
    tvAndMoviesCast.foreach(println)

    // creating an RDD that groups two different RDDs by the same key
    val castWithTvAndMovies = actorsAndMovies.cogroup(actorsAndShows)
    castWithTvAndMovies.foreach(println)

  }
}

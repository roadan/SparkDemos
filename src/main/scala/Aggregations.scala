import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * this demo shows a simple aggregations over RDDs
 */
class Aggregations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  }

}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)

    val files = sc.textFile("../../books/The Complete Works of William Shakespeare by William Shakespeare.txt")

    val words = files.flatMap(line => line.split(" ")).filter(s => s != null && !s.isEmpty)
    val count = words.map(word => (word, 1)).reduceByKey(_ + _)
    val sorted = count.map(t => (t._2, t._1)).sortByKey(false)

    sorted.foreach(print)

  }

}

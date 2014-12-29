
import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._

/**
 * This demo shows streaming data from twitter as well as a
 * reduceByKeyAndWindow operation
 */
object TwitterStream {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StreamingTwitter")
    // sparkConf.setMaster("spark://ec2-54-77-55-253.eu-west-1.compute.amazonaws.com:7077")

    StreamingExamples.setStreamingLogLevels()

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "eyzYUFBz7t6c9SBy7XRSCxtZI")
    System.setProperty("twitter4j.oauth.consumerSecret", "OIBIXqpepXGMvidQIYDhZH0teq61vEOrxLVGLVg237yfjQ8Tzb")
    System.setProperty("twitter4j.oauth.accessToken", "352922313-7reCJok6u30oZU0ZULEqSRyVcvIbiYJhnMYUl5J8")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "1ovFiMUDrOFUazyRKYVTpsSn4GkZuWxpZKsfkf5YKdUs7")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val tweetStream = TwitterUtils.createStream(ssc, None, args, StorageLevel.MEMORY_ONLY_SER_2 )

    val sparkTweets = tweetStream.map(twt => (twt.getUser.getName, twt.getText.toLowerCase) //&&
                                                 // twt.getText.toLowerCase.contains("#apachespark") &&
                                                 //twt.getText.toLowerCase.contains("@yrodenski")
    )

    // lets transform our DStream to a key-value pier so we can
    // aggregate later
    //val userTweets = sparkTweets.map(t => (t.getUser.getName, t.getText))

    // first, lets print the tweets so I can give one of you a book
    sparkTweets.foreachRDD(rdd => {

      rdd.collect.foreach(t=>{
        println("---------- start tweet ----------")
        println(t)
        println("---------- end tweet ----------")
      })

    })

    println("------------------ userTweets is running ---------------------")
    // lets aggregate!
    val tweetCount = sparkTweets.reduceByKeyAndWindow(_ + " " + _, Seconds(30))
                                .map( x => (x._2.split("").length, x._1))


    tweetCount.foreachRDD(rdd => {
      println("****************** Start Window ******************")
      rdd.sortByKey(false).collect.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}

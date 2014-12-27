import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * Created by roadan on 12/27/14.
 */
class TwitterStream {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StreamingTwitter")
    // sparkConf.setMaster("spark://Spark0:7077")
    sparkConf.setExecutorEnv("spark.executor.memory","4G")

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "eyzYUFBz7t6c9SBy7XRSCxtZI")
    System.setProperty("twitter4j.oauth.consumerSecret", "OIBIXqpepXGMvidQIYDhZH0teq61vEOrxLVGLVg237yfjQ8Tzb")
    System.setProperty("twitter4j.oauth.accessToken", "352922313-7reCJok6u30oZU0ZULEqSRyVcvIbiYJhnMYUl5J8")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "1ovFiMUDrOFUazyRKYVTpsSn4GkZuWxpZKsfkf5YKdUs7")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val tweetStream = TwitterUtils.createStream(ssc, None, args, StorageLevel.MEMORY_ONLY_SER_2 )

    val sparkTweets = tweetStream.filter( twt => twt.getHashtagEntities.contains("SDP14") &&
                                               twt.getHashtagEntities.contains("ApacheSpark") &&
                                               twt.getUserMentionEntities().contains("@YRodenski"))

    // lets transform our DStream to a key-value pier so we can
    // aggregate later
    val userTweets = sparkTweets.map(t => (t.getUser().getName(), t))

    // first, lets print the tweets so I can give one of you a book
    userTweets.foreachRDD(rdd => rdd.collect.foreach(println))

    // lets aggregate!


    ssc.start()
    ssc.awaitTermination()
  }

}

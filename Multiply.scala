

package edu.uta.cse6331

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

 
object Multiply {
  
  
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }

 def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  def main(args: Array[String]) {

    
   setupTwitter()
    
    

    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    
    setupLogging()

    // Creating Dstream
    val tweets = TwitterUtils.createStream(ssc, None)
    
    
    val statuses = tweets.map(status => status.getText())
    
    
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(600), Seconds(1))
    
    
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
    sortedResults.print
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}


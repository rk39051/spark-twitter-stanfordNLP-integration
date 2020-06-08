package com.spwitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object SparkTwitterSentiment {
  def main(args:Array[String]){
    
    System.setProperty("hadoop.home.dir","D:\\winutils");
    val log=Logger.getLogger("org")
    log.setLevel(Level.ERROR);
    
    if(args.length<4)
    {
      println("please enter valid twitter credential consumerKey, "+ 
               "consumerSecret, accessToken, accessTokenSecret respectively\n");
      System.exit(1);
    }
    
    val Array(consumerKey,consumerSecret, accessToken, accessTokenSecret )=args.take(4);
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
    
    //topic for filtration of data
    val filters = args.takeRight(args.length - 4) 
    
    //creating spark conf
    val conf=new SparkConf().setAppName("twitter hashtag app").setMaster("local[2]");
    //creating streaming context
    val ssc=new StreamingContext(conf,Seconds(30));
    //creating stream of twitter data
    val stream=TwitterUtils.createStream(ssc,None, filters);
    val filtered_stream=stream.filter(_.getLang()=="en")
    //filtering twitter hashTag
    val tweets=filtered_stream.map(status=>{
      val text=status.getText();
      val sentiment=StanfordNlpSentiment.getSentiment(text)
       (sentiment,text)
      
    });
    tweets.foreachRDD(rdd=>{
    //rdd.foreach{case(tag,text) => println("%s ( %s)".format(tag.mkString(""),text))}
    val spark=SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate();
     import spark.implicits._
     val df=rdd.toDF("sentiment","text");
     df.show(false);
     
    
    })
    
    ssc.start();
    ssc.awaitTermination();
}

}
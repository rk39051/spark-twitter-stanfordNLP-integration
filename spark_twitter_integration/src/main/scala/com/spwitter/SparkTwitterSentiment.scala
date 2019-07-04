package com.spwitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level

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
    val conf=new SparkConf().setAppName("twitter hashtag app").setMaster("local[*]");
    //creating streaming context
    val ssc=new StreamingContext(conf,Seconds(20));
    //creating stream of twitter data
    val stream=TwitterUtils.createStream(ssc,None, filters);
    val filtered_stream=stream.filter(_.getLang()=="en")
    //filtering twitter hashTag
    val tweets=filtered_stream.map(status=>{
      val tag=status.getHashtagEntities.map(_.getText.toString().toUpperCase())
      val text=status.getText();
      val sentiment=StanfordNlpSentiment.getSentiment(text)
       (tag,sentiment,text)
      
    });
    tweets.foreachRDD(rdd=>{
    //rdd.foreach{case(tag,text) => println("%s ( %s)".format(tag.mkString(""),text))}
     rdd.foreach{case(tag,sentiment,text) => println(tag.mkString("")+"\n"+"sentiment:"+sentiment+" text:"+text)}
    })
    
    ssc.start();
    ssc.awaitTermination();
}

}
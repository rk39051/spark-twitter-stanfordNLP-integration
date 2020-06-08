package com.spwitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds

object SparkTwitterHashTag {
    def main(args:Array[String]){
    
    System.setProperty("hadoop.home.dir","D:\\winutils");
    Logger.getLogger("org").setLevel(Level.ERROR);
    
   
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
    
    //topic for filtration of data from twitter
    val filters = args.takeRight(args.length - 4) 
    
    //creating spark conf
    val conf=new SparkConf().setAppName("twitter hashtag app").setMaster("local[*]");
    //creating streaming context
    val ssc=new StreamingContext(conf,Seconds(30));
    //creating stream of twitter data
    val stream=TwitterUtils.createStream(ssc,None, filters);
    //filtering twitter hashTag
    val hashTag=stream.filter(_.getLang()=="en").flatMap(x=>x.getText().split(" ").filter(_.startsWith("#")));
    val hashTagMap=hashTag.map(x=>(x,1));
    
    //creating window of length 60 sec
    val hashCount=hashTagMap.reduceByKey((x:Int,y:Int)=>x+y);
   
    
    
    hashCount.foreachRDD(rdd=>{
      //sorting rdd based on value
      val rddSort=rdd.sortBy(x=>x._2, false);
      println("\nshowing top 10 Popular topics in last 30 seconds (%s total):".format(rdd.count()))
      val topHashTag=rddSort.take(10);
      //topHashTag.foreach{case (tag, count) => println("%s,%s".format(tag, count))}
      topHashTag.foreach{case (tag, count) => println(tag+","+count)}
      
    }) 
   
    ssc.start();
    ssc.awaitTermination();
    

  }

}
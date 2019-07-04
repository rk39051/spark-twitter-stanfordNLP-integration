package com.spwitter

import java.util.Properties
import scala.collection.JavaConverters._

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.util.logging.RedwoodConfiguration

object StanfordNlpSentiment {
   def getSentiment(text:String):String ={
   
    val props:Properties=new Properties();
   RedwoodConfiguration.current().clear().apply();
    props.put("annotators", "tokenize, ssplit, parse, sentiment");
	  val document=new Annotation(text);
	  val pipeline=new StanfordCoreNLP(props);
		pipeline.annotate(document);
		val sentences:List[CoreMap]=document.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toList
		var longest=0;
		var mainSentiment=0;
		var str="";
		for(sentence <- sentences) {
			var tree:Tree=sentence.get(classOf[SentimentAnnotatedTree]);
			var sentiment=RNNCoreAnnotations.getPredictedClass(tree);
			var partText=sentences.toString();
			 if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
			}
		
		}
		if( mainSentiment<=1){
		  str="negative"
		}
		if( mainSentiment==2){
		  str="neutral"
		}
		if( mainSentiment>2 ){
		  str="positive"
		}
		  return str;	
	}
}
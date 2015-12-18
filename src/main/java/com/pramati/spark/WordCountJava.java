package com.pramati.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import java.util.Arrays;
import scala.Tuple2;


public class WordCountJava {
  
   public static void main(String args[])

     { 
            SparkConf conf = new SparkConf();

            conf.setMaster("local").setAppName("wordCount");

            JavaSparkContext jsc = new JavaSparkContext(conf);

            JavaRDD<String> lines = jsc.textFile("/opt/software/spark-1.3.1-bin-hadoop2.6/README.md");
            System.out.println(lines.count());

             JavaPairRDD<String, Integer> pairRDD =
                lines.flatMap(line -> Arrays.asList(line.split(" "))).mapToPair(word -> new Tuple2(word, 1));

             System.out.println(pairRDD.reduceByKey((a, b) -> a + b).collectAsMap());


            JavaRDD<String> words =  lines.flatMap(new FlatMapFunction<String,String>(){
                   public Iterable<String> call(String line) {
                   return Arrays.asList(line.split(" ")); 
                   }
                   });

           JavaPairRDD<String,Integer> wordpair = words.mapToPair(new PairFunction<String,String,Integer>(){
                 public Tuple2<String,Integer> call(String word) {
                    return new Tuple2<String,Integer>(word,1);
                  } 
               });

           JavaPairRDD<String,Integer> wordCount = wordpair.reduceByKey(new Function2<Integer,Integer,Integer>(){
                 public Integer call(Integer a, Integer b) { return a + b; }
                });

            System.out.println(wordCount.collectAsMap());

            jsc.stop();
         } 

}

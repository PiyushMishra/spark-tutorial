package com.pramati.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;


/**
 * Created by piyushm on 2/9/15.
 */
public class AggregateJava {

    public static void main(String args[]) {

        SparkConf conf = new SparkConf().setAppName("AggregateJava").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> listRDD = jsc.parallelize(Arrays.asList(1,2,3));

        System.out.println(listRDD.map(i -> i +1).collect());

        Function<Integer, Integer> incrementByOne =  new Function<Integer, Integer>() {
            public Integer call(Integer i)
            {
                return i+1;
            }
        };

        JavaRDD<Integer> rdd  = listRDD.map(incrementByOne);

        System.out.println(rdd.collect());


        JavaRDD<String> lines = jsc.textFile("src/main/resources/README.md");
        JavaRDD fLines = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String line)
            {
                return line.contains("Python");
            }
        });

        System.out.println("fLines  " + fLines.collect());

        JavaRDD<String> ffLines  = lines.filter(line -> line.contains("Python"));

        System.out.println("ffLines   " + ffLines.collect());

      // wordCount

     




        jsc.stop();

    }

}

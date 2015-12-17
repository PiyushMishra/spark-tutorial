package com.pramati.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


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

      // wordCount with anonymous functions

        System.out.print(lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String,String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        }).collectAsMap());

        JavaPairRDD<String, Integer> pairRDD =
                lines.flatMap(line -> Arrays.asList(line.split(" "))).mapToPair(word -> new Tuple2(word, 1));

        System.out.println(pairRDD.reduceByKey((a, b) -> a + b).collectAsMap());


//        JavaPairRDD<String, String> javaPairRDD1 = lines.mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) {
//                System.out.println(s);
//                return new Tuple2(s.split(" ")[0], s);
//            }
//        });
//
//        JavaPairRDD<String, String> javaPairRDD2 = lines.mapToPair(a -> new Tuple2(a.split(" ")[0], a));
//
//        System.out.println(javaPairRDD1.reduceByKey((a, b) -> a + b).collectAsMap());
//
//        System.out.println(javaPairRDD2.reduceByKey((a, b) -> a + b).collectAsMap());

        Tuple2[] arr = new Tuple2[3];
        arr[0] = new Tuple2(1,2);
        arr[1] = new Tuple2(2,3);
        arr[2] = new Tuple2(3,6);


        Tuple2[] arr1 = new Tuple2[1];
        arr[2] = new Tuple2(3,9);

       JavaRDD<Tuple2<Integer, Integer>> j = jsc.parallelize(Arrays.asList(arr));
        System.out.println(j.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2._1() > 1;
            }
        }).collect());

        System.out.println(j.filter(a -> a._1() > 1).collect());

        jsc.stop();

    }

}

package com.pramati.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by piyushm on 21/10/15.
 */
public class KeyValue {


    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("KeyValueTest").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, Integer> pairs = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
                new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6)));

        JavaPairRDD<Integer, Integer> pairs1 = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(3, 9)));

        println("pairs reduceByKey  " + pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).collect());


        println("pairs groupByKey " + pairs.groupByKey().collect());

        println("pairs map values " + pairs.mapValues(new Function<Integer, Integer>() {
            public Integer call(Integer integer) {
                return integer +1 ;
            }
        }).collect());

        println("pairs flatMap values" + pairs.flatMapValues(new Function<Integer, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Integer integer) throws Exception {
                return Arrays.asList(integer, integer +1);
            }
        }).collect());

        println("pairs keys " + pairs.keys().collect());

        println("pairs sortBy Key " + pairs.sortByKey().collect());

        println("pairs values " + pairs.values().collect());

        println("pairs subtractByKey " + pairs.subtractByKey(pairs1).collect());
        println("pairs join " + pairs.join(pairs1).collect());
        println("pairs rightOuterJoin " + pairs.rightOuterJoin(pairs1).collect());
        println("pairs leftOuterJoin " + pairs.leftOuterJoin(pairs1).collect());
        println("pairs cogroup " + pairs.cogroup(pairs1).collect());




    }

    public static void println(Object obj) {
        System.out.println(obj.toString());
    }


}

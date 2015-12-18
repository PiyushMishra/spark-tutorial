package com.pramati.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SparkIntro {

    public static void main(String args[]) {

        Integer arr[] = new Integer[]{1, 2, 3, 4};
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("intro");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(arr));
        System.out.println("hello " + rdd.filter(a -> a > 1).collect());
        System.out.println("hello " + rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) {
                return integer > 1;
            }
        }).collect());


        JavaRDD<String> lines = jsc.textFile("/opt/software/spark-1.3.1-bin-hadoop2.6/README.md");

        println(lines.collect());

        println(lines.filter(a -> a.contains("Python")).collect());

        println(lines.filter(new Function<String, Boolean>() {

            @Override
            public Boolean call(String s) {

                return s.contains("Python");

            }

        }).collect());

        JavaRDD<String> listlinesRDD = jsc.parallelize(Arrays.asList("hello", "world"));
        println(listlinesRDD.filter(a -> a.contains("ell")).collect());
        println(listlinesRDD.filter(new Function<String, Boolean>() {
                                        @Override
                                        public Boolean call(String line) {
                                            return line.contains("ell");
                                        }
                                    }
        ).collect());

        JavaRDD<String> log = jsc.textFile("./log.txt");
        JavaRDD<String> errors1 = log.filter(a -> a.contains("error"));
        println(errors1.collect());
        println("*********" + log.filter(new Function<String, Boolean>() {
            public Boolean call(String line) {
                return line.contains("error");
            }
        }).collect());


        JavaRDD<String> warning = log.filter(new Function<String, Boolean>() {
            public Boolean call(String line) {
                return line.contains("warning");
            }
        });

        println("&&&&&&&&&&&&&&&&&&" + errors1.union(warning).collect());

        println(errors1.count());

        for (String line : errors1.take(3)) {
            println(line);

        }

        println("#################" + log.filter(new ContainsError()).collect());
        println("#################" + log.filter(new Contains("warning")).collect());


        JavaRDD<Integer> nums = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        List<Integer> values = nums.map(new Function<Integer, Integer>() {
            public Integer call(Integer i) {
                return i * i;
            }
        }).collect();


        for (Integer v : values) {
            println(v);
        }

        JavaRDD<String> lines1 = jsc.parallelize(Arrays.asList("Hello world", "hi john"));
        JavaRDD<String> words1 = lines1.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) {
                return Arrays.asList(line.split(" "));
            }
        });

        println(words1.collect());


        JavaRDD<String> rdd1 = jsc.parallelize(Arrays.asList("coffee", "coffee", "panda", "monkey", "tea"));
        JavaRDD<String> rdd2 = jsc.parallelize(Arrays.asList("coffee", "monkey", "kitty"));
        println("distinct" + rdd1.distinct().collect());
        println("intersection" + rdd1.intersection(rdd2).collect());
        println("union" + rdd1.union(rdd2).collect());
        println("subtract" + rdd1.subtract(rdd2).collect());

        JavaRDD<Integer> integerRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 3));
        println("integer map RDD " + integerRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer x) {
                return x + 1;
            }
        }).collect());

        println("integer flatMap RDD " + integerRDD.flatMap(new FlatMapFunction<Integer, Integer>() {
            public Iterable<Integer> call(Integer x) {
                return Arrays.asList(1, x);
            }
        }).collect());

        println("filter integer RDD " + integerRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer != 1;
            }
        }).collect());


        println("distinct " + integerRDD.distinct().collect());
        println("sample" + integerRDD.sample(false, 0.5).collect());


        JavaRDD<Integer> int1 = jsc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> int2 = jsc.parallelize(Arrays.asList(3, 4, 5));

        println("union " + int1.union(int2).collect());
        println("intersection " + int1.intersection(int2).collect());
        println("subtract " + int1.subtract(int2).collect());
        println("cartesian " + int1.cartesian(int2).collect());


        JavaRDD<Integer> commonRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 3));
        println("collect " + commonRDD.collect());
        println("count is " + commonRDD.count());
        println("count by value " + commonRDD.count());
        println("take " + commonRDD.take(2));
        println("top " + commonRDD.top(2));
        println("takeOrdered " + commonRDD.takeOrdered(2));
        println("take sample " + commonRDD.takeSample(false, 1));
        println("reduce " + commonRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }));
        println("fold " + commonRDD.fold(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }));

        commonRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                println(integer);
            }
        });

        // aggregate by example

        class Average implements Serializable {
            Integer total;
            Integer num;

            public Average(Integer total, Integer num) {
                this.total = total;
                this.num = num;
            }

            public float avg() {
                return total.floatValue() / num.floatValue();
            }
        }

        class AddAndCount implements Function2<Average, Integer, Average> {
            public Average call(Average avg, Integer x) {
                avg.total = avg.total + x;
                avg.num = avg.num + 1;
                return new Average(avg.total, avg.num);
            }
        }

        class Combine implements Function2<Average, Average, Average> {
            public Average call(Average avg1, Average avg2) {
                avg1.total = avg1.total + avg2.total;
                avg1.num = avg1.num + avg2.num;
                return new Average(avg1.total, avg2.num);
            }
        }


        Average avg = new Average(0, 0);
        Combine c = new Combine();
        AddAndCount ac = new AddAndCount();
        Average avg1 = commonRDD.aggregate(avg, ac, c);
        println(avg1.avg());

        println("flat map double " + commonRDD.flatMapToDouble(new DoubleFlatMapFunction<Integer>() {
            public Iterable<Double> call(Integer x) {
                return Arrays.asList(x.doubleValue(), x.doubleValue() + 1.0);
            }
        }).collect());


        println("map to double " + commonRDD.mapToDouble(new DoubleFunction<Integer>() {
            @Override
            public double call(Integer integer) throws Exception {
                return integer.doubleValue();
            }
        }).collect());


        JavaPairRDD<Integer, String> pairs = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer, String>(1, "one"),
                new Tuple2<Integer, String>(2, "two")));
        println("pair flat map function" + pairs.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, String>, Integer, String>() {
            @Override
            public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return Arrays.asList(integerStringTuple2);
            }
        }).collect());

        println("map to pair " + commonRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer integer) {

                return new Tuple2<Integer, Integer>(integer, integer + 1);

            }
        }).collect());

        jsc.stop();
    }

    public static class ContainsError implements Function<String, Boolean> {
        public Boolean call(String line) {
            return line.contains("error");
        }
    }

    public static class Contains implements Function<String, Boolean> {
        private String query;

        public Contains(String query) {
            this.query = query;
        }

        public Boolean call(String line) {
            return line.contains(query);
        }
    }

    public static void println(Object obj) {
        System.out.println(obj.toString());
    }

}

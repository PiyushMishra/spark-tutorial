package com.pramati.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import java.io.StringReader;
import org.apache.spark.api.java.function.*;
import au.com.bytecode.opencsv.CSVReader;
/**
 * Created by piyushm on 10/11/15.
 */
public class loadData {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("KeyValueTest").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("/home/piyushm/samplejson.json");
        List<Person> persons = lines.mapPartitions(new ParseJson()).collect();
        JavaRDD<Person> personJavaRDD = jsc.parallelize(persons);
        JavaRDD<String> csvFileContent = jsc.textFile("/opt/sample.csv");
        System.out.println(csvFileContent.map(new ParseLine()).collect());
        System.out.println(persons);
        System.out.println(personJavaRDD.mapPartitions(new WriteJson()).collect());
        jsc.stop();
    }
}

class ParseLine implements Function<String, String[]> {
    @Override
    public String[] call(String s) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(s));
        return reader.readNext();
    }
}
class Person implements Serializable {
    String name;
    String lovepandas;

    public String getLovepandas() {
        return lovepandas;
    }

    public void setLovepandas(String lovepandas) {
        this.lovepandas = lovepandas;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "name " + name + " lovepandas " + lovepandas;
    }
}

class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
    @Override
    public Iterable<Person> call(Iterator<String> strings) throws Exception {
        ArrayList<Person> persons = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        while (strings.hasNext()) {
            String line = strings.next();
            Person person = mapper.readValue(line, Person.class);
            persons.add(person);
        }
        return persons;
    }

}

class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
    @Override
    public Iterable<String> call(Iterator<Person> persons) throws Exception {
        ArrayList<String> personJson = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        while (persons.hasNext()) {
            Person line = persons.next();
            String person = mapper.writeValueAsString(line);
            personJson.add(person);
        }
        return personJson;
    }
}
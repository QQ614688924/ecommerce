package com.atguigu.userprofile.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class HotWordEtl {

    public static void main(String[] args) {

        SparkConf sc = new SparkConf().setAppName("hot word etl").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sc);

//        System.setProperty("HADOOP_USER_NAME","centos7");

        JavaRDD<String> linesRdd = jsc.textFile("hdfs://hadoop102:8020/data/SogouQ.sample.txt");


        JavaPairRDD<String, Integer> wordRdd = linesRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                String word = s.split("\t")[2];
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceRdd = wordRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> swapRdd = reduceRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> sortRdd = swapRdd.sortByKey(false);

        JavaPairRDD<String, Integer> mapRdd = sortRdd.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        List<Tuple2<String, Integer>> result = mapRdd.take(10);

        for (Tuple2<String, Integer> hotWordCount : result) {
            System.out.println(hotWordCount._1 + "===" + hotWordCount._2);
        }


    }

}

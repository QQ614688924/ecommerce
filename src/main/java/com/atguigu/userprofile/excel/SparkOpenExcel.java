package com.atguigu.userprofile.excel;

import com.atguigu.userprofile.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkOpenExcel {

    public static void main(String[] args) {

//        SparkSession session = SparkUtils.initSession();
//
//        Dataset<Row> load = session.read().format("com.crealytics.spark.excel")
//                .option("header", "true")
//                .load("hdfs://hadoop102:8020/excel/orginal_mobile.xlsx");


        SparkConf sc = new SparkConf().setAppName("hot word etl").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> stringJavaRDD = jsc.textFile("hdfs://hadoop102:8020/excel/orginal_mobile.xlsx");



    }

}

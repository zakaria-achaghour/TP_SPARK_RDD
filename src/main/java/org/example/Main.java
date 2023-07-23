package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setAppName("TP ventes").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String> rddLines = sc.textFile("ventes.txt");
        JavaPairRDD<String, Double> rddVent = rddLines.mapToPair(new PairFunction<String, String, Double>() {

            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] strings = s.split(" ");
                String city = strings[1];
                Double price = Double.parseDouble(strings[3]);
                String date[]=strings[0].split("-");
                String year=date[2];
                return new Tuple2<>(city+","+year, price);
            }
        });
        List<Tuple2<String, Double>> ventes = rddVent.reduceByKey((aDouble, aDouble2) -> aDouble + aDouble2).collect();

        for (Tuple2<String,Double> vent:ventes) {
            System.out.println(vent._1+" "+vent._2);
        }

    }
}
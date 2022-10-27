package com.fairbanks;

import java.util.Arrays;

import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


@Log4j
public class Main {

    public static void main(String[] args) {
        /*
          Configures Spark
         */
        SparkConf conf = new SparkConf()
            .setAppName("Spark 4 Java Learning")
            .setMaster("local[*]"); // [*] means it will use all cores from the machine

        /*
          Sets the Spark Context
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("src/main/resources/subtitles/input.txt")
            .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase().trim())
            .filter(sentence -> !sentence.isEmpty())
            .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
            .filter(sentence -> !sentence.trim().isEmpty())
            .filter(Util::isNotBoring)
            .mapToPair(word -> new Tuple2<>(word, 1L))
            .reduceByKey(Long::sum)
            .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
            .sortByKey(false)
            .take(50)
            .forEach(word -> log.info("" + word));

        sc.close();

    }

}

package com.fairbanks;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


@Log4j
public class Main {

    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

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

        sc.parallelize(inputData)
            .mapToPair(logEntry -> new Tuple2<>(logEntry.split(":")[0], 1L))
            .reduceByKey(Long::sum)
            .foreach(tuple -> log.info("LogLevel " + tuple._1() + " has " + tuple._2() + " instances"));

        sc.close();

    }

}

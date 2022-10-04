package com.fairbanks;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


@Log4j
public class Main {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

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

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        // Experimenting with Reduce
        Integer result = myRdd.reduce(Integer::sum);
        log.info("Reduce Result: " + result);

        // Experimenting with Mapping
        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
        log.info("Map Result: ");
        sqrtRdd.foreach(element -> log.info("\t - " + element));
        sc.close();
    }

}

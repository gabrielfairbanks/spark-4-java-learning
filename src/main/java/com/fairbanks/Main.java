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
        List<Double> inputData = new ArrayList<>();
        inputData.add(1.0);
        inputData.add(2.0);
        inputData.add(1.0);
        inputData.add(5.0);
        inputData.add(1.0);

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

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        // Experimenting with Reduce
        Double result = myRdd.reduce((value1, value2) -> value1 + value2); // It would be easier to write reduce(Double::sum), but leaving like this for educational purposes

        log.info("Reduce Result: " + result);

        sc.close();
    }

}

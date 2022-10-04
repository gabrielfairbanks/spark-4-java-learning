package com.fairbanks;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Main {

    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(8.4);
        inputData.add(1893.45643);
        inputData.add(2388.6456);
        inputData.add(8467230923.94);

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
        myRdd.reduce((value1, value2) -> value1 + value2);

        sc.close();
    }

}

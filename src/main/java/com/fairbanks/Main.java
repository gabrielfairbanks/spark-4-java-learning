package com.fairbanks;

import java.util.Arrays;

import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


@Log4j
public class Main {

    public static void main(String[] args) {
        /*
           Configures Hadoop
         */
        System.setProperty("hadoop.home.dir", "D:\\code\\spark-4-java-learning\\course materials\\Practicals\\winutils-extra\\hadoop");

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
            .filter(Util::isNotBoring)
            .take(50)
            .forEach(word -> log.info("" + word));

        sc.close();

    }

}

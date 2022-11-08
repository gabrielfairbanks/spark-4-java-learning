package com.fairbanks;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
@Log4j
public class ViewingFigures {


    @SuppressWarnings("resource")
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

        log.info("Listing chapter data:");
        JavaPairRDD<Integer, Integer> courseChapterCount = chapterData.mapToPair(chapterEntry -> new Tuple2<>(chapterEntry._2(), 1))
            .reduceByKey(Integer::sum);

        log.info("Listing view data: ");
        viewData.distinct()
            .mapToPair(viewEntry -> new Tuple2<>(viewEntry._2(), viewEntry._1()))
            .join(chapterData)
            .mapToPair(viewEntry -> new Tuple2<>(viewEntry._2(), 1))
            .reduceByKey(Integer::sum)
            .mapToPair(viewEntry -> new Tuple2<>(viewEntry._1()._2(), viewEntry._2()))
            .join(courseChapterCount)
            .mapToPair(courseViewEntry -> new Tuple2<>(courseViewEntry._1(), (double) courseViewEntry._2()._1() / courseViewEntry._2()._2()))
            .mapToPair(ViewingFigures::calculateCourseScores)
            .reduceByKey(Integer::sum)
            .join(titlesData)
            .mapToPair(viewEntry -> new Tuple2<>(viewEntry._2()._2(), viewEntry._2()._1()))
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .mapToPair(Tuple2::swap)
            .take(100)
            .forEach(viewEntry -> log.info(viewEntry._1() + ", " + viewEntry._2()));

        sc.close();
    }


    private static Tuple2<Integer, Integer> calculateCourseScores(Tuple2<Integer, Double> courseViewsPercentage) {
        int score = 0;
        double percentage = courseViewsPercentage._2();
        int course = courseViewsPercentage._1();

        if (percentage >= 0.9) {
            score = 10;
        } else if (percentage >= 0.5) {
            score = 4;
        } else if (percentage >= 0.25) {
            score = 2;
        }
        return new Tuple2<>(course, score);
    }


    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
            .mapToPair(commaSeparatedLine -> {
                String[] cols = commaSeparatedLine.split(",");
                return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
            });
    }


    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
            .mapToPair(commaSeparatedLine -> {
                String[] cols = commaSeparatedLine.split(",");
                return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
            });
    }


    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
            .mapToPair(commaSeparatedLine -> {
                String[] columns = commaSeparatedLine.split(",");
                return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
            });
    }
}

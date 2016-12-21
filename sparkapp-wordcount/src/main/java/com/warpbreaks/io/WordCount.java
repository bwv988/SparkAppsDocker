// Keeping this simple on purpose, so no error checking.

package com.warpbreaks.io;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;


public class WordCount {
    private static final String APP_NAME;

    static {
        APP_NAME = "com.warpbreaks.io.WordCount";
    }

    public static void main(String[] args) {
        /*
        When running locally on Windows, we need to set the path to the winutils:

        System.setProperty("hadoop.home.dir", "C:\\Utils\\winutils\\");
        */

        if (args.length != 3) {
            System.err.println("Usage: WordCount <spark master> <input file> <output dir>");
            System.exit(0);
        }

        String sparkMaster = args[0];
        System.out.println("Running on Spark master: " + sparkMaster);

        // Initialize Spark.
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(sparkMaster);

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read file.
        JavaRDD<String> lines = sc.textFile(args[1]);
        JavaRDD<String> words =
                lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Count words.
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((x, y) -> x + y);

        counts.saveAsTextFile(args[2]);
    }
}
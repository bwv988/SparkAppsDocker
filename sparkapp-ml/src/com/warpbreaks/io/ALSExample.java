package com.warpbreaks.io;

import org.apache.spark.sql.SparkSession;


public class ALSExample {
    private static final String APP_NAME;
    private static String dataPath;

n    private SparkSession spark;

    static {
        APP_NAME = "ALSExample";
        DATA_FILE = "/home/sral/Temp/movielens.txt";
    }

    static void initializeApp(String[] args) {
        if (args.length != 1) {
            System.err.format("Usage: %s <spark master>", APP_NAME);
            System.exit(0);
        }

        String sparkMaster = args[0];

        spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master(sparkMaster)
                .getOrCreate();
    }

    public static void main(String[] args) {
        initializeApp(args);
    }
}
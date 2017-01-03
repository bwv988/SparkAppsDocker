// Very small example that shows how to access Hive through the Spark session.
// RS22122016
//
// Prerequisite: See README.md!


package com.warpbreaks.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;

public class HiveApp {
    private static final String APP_NAME;
    private static final String WAREHOUSE_LOC;
    private static final String CSV_FILE;

    static {
        APP_NAME = "com.warpbreaks.io.HiveApp";
        WAREHOUSE_LOC = "/user/hive/warehouse";     // Location of Hive's warehouse dir in HDFS.
        CSV_FILE = "hdfs://namenode:8020/data/scarlatti_sonatas.csv";
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: HiveApp <spark master>");
            System.exit(0);
        }

        String sparkMaster = args[0];

        // Initialize Spark session.
        SparkSession spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master(sparkMaster)
                .config("spark.sql.warehouse.dir", WAREHOUSE_LOC)
                .enableHiveSupport()
                .getOrCreate();

        // Read data from CSV file.
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_FILE);
        df.show();

        // Let's now save the data frame to Hive.
        spark.sql("DROP TABLE IF EXISTS scarlatti");
        DataFrameWriter<Row> out = df.write();
        out.saveAsTable("scarlatti");

        long numMinor = spark.sql("SELECT K from scarlatti WHERE Key LIKE '%minor'").count();
        long numMajor = spark.sql("SELECT K from scarlatti WHERE Key LIKE '%major'").count();

        System.out.format("\n\nNumber of Scarlatti sonatas\n ---> in minor: %d\n ---> in major: %d\n\n", numMinor, numMajor);

        spark.stop();
        System.exit(0);
    }
}
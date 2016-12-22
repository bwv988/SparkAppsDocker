// Very small example that shows how to connect to Hive via Spark, create a table and perform queries.
// RS22122016
//
// Prerequisite: See readme!


package com.warpbreaks.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        // First, let's create a simple table in Hive.
        /*spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");

        // Create a few rows.
        Random rndGen = new Random();
        for (int i = 0; i < 10; i++) {
            int id = rndGen.nextInt(100);

            String sqlQuery = String.format("INSERT INTO src VALUES (%d, 'Hello Hive from Java - %d')", id, id);
            System.out.println("Query: " + sqlQuery);
            spark.sql(sqlQuery)sparspark;
        }

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src").show();


        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM src").show();

        // The results of SQL queries are themselves DataFrames and support all normal functions.
        spark.sql("SELECT key, value FROM src WHERE key < 5 ORDER BY key").show();
*/

        spark.stop();
        System.exit(0);
    }
}
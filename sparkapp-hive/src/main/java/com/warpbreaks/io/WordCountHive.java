package com.warpbreaks.io;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WordCountHive {
    private static final String APP_NAME;

    static {
        APP_NAME = "com.warpbreaks.io.WordCountHive";
    }

    // $example on:spark_hive$
    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static void main(String[] args) {
        /*
        When running locally on Windows, we need to set the path to the winutils:

        System.setProperty("hadoop.home.dir", "C:\\Utils\\winutils\\");
        */
        System.out.println("Apache Spark WordCountHive starting...");

        if (args.length != 3) {
            System.err.println("Usage: WordCountHive <spark master> <inputfile> <output directory>");
            System.exit(0);
        }

        String sparkMaster = args[0];
        System.out.println("Running on Spark master: " + sparkMaster);


        String warehouseLocation = "/user/hive/warehouse";
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        spark.sql("INSERT INTO src VALUES (1, 'Hello world!");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src").show();


        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM src").show();

        // The results of SQL queries are themselves DataFrames and support all normal functions.
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

        // The items in DaraFrames are of type Row, which lets you to access each column by ordinal.
        Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Key: " + row.get(0) + ", Value: " + row.get(1);
            }
        }, Encoders.STRING());
        stringsDS.show();

        // You can also use DataFrames to create temporary views within a SparkSession.
        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        // Queries can then join DataFrames data with data stored in Hive.
        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();


        spark.stop();

        System.exit(0);
    }
}
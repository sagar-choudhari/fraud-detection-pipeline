package com.frauddetection.utils;

import com.frauddetection.spark.SparkSessionFactory;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

//Added for to visualise the data
public class DeltaReader {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("Delta Reader");


        Dataset<Row> data = spark.read()
                .format("delta")
                .load("delta/transactions");

        data.write().mode(SaveMode.Overwrite).option("header", true).csv("C:/Users/sgrch/Desktop/csv");

        DeltaTable table = DeltaTable.forPath(spark, "delta/transactions");

        table.history()
                        .select(
                                "version",
                                "timestamp",
                                "operation",
                                "operationParameters",
                                "username",
                                "clusterId",
                                "operationMetrics"
                        ).write().mode(SaveMode.Overwrite).option("header", true).json("C:/Users/sgrch/Desktop/transaction_history");

        table.history().printSchema();
    }
}

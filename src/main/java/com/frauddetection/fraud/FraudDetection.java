package com.frauddetection.fraud;

import com.frauddetection.spark.SparkSessionFactory;
import com.frauddetection.utils.ConfigLoader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
public class FraudDetection {

    private static final String DELTA_PATH = ConfigLoader.get("delta.transactions.path");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.create("Fraud Detector");

        Dataset<Row> transactions = spark.read()
                .format("delta")
                .load(DELTA_PATH)
                .withColumnRenamed("userId", "user_id")
                .withColumnRenamed("txnId", "txn_id");

        System.out.println("Total number of transactions...................: "+transactions.count());

        // -----------------------------------------
        // RULE 1 — High Value Transaction (> 8000)
        // -----------------------------------------
        System.out.println("High Value transactions.........................");
        Dataset<Row> highValues = transactions.filter(col("amount").gt(8000))
                .withColumn("fraud_flag", lit("HIGH_VALUE"))
                .select("txn_id", "user_id", "amount", "merchant", "timestamp", "fraud_flag");

        highValues.show(5, false);

        // -----------------------------------------
        // RULE 2 — Card Testing (same user, same merchant, > 2 times)
        // -----------------------------------------
        System.out.println("Possible Card Testing Attack.........................");

        WindowSpec cardTestWindow = Window.partitionBy("user_id", "merchant");

        Dataset<Row> cardTesting = transactions
                .withColumn("txn_count", count("txn_id").over(cardTestWindow))
                .filter(col("txn_count").gt(2))
                .withColumn("fraud_flag", lit("CARD_TESTING"))
                .select("txn_id", "user_id", "amount", "merchant", "timestamp", "fraud_flag");

        cardTesting.show(5, false);

        // -----------------------------------------
        // RULE 3 — Velocity Check (> 3 transactions per user within 1 hour)
        // -----------------------------------------
        System.out.println("High Velocity transactions.........................");
        Dataset<Row> velocity = transactions
                .groupBy(col("user_id"), window(col("timestamp"), "1 hour"))
                .agg(
                        count(col("txn_id")).alias("txn_count"),
                        round(sum(col("amount")), 2).alias("total_amount")
                )
                .filter(col("txn_count").gt(3))
                .withColumn("fraud_flag", lit("HIGH_VELOCITY"))
                .select("user_id", "window", "txn_count", "total_amount", "fraud_flag")
                .orderBy(col("txn_count").desc());

        velocity.show(5, false);

        SparkSessionFactory.stop();

    }
}

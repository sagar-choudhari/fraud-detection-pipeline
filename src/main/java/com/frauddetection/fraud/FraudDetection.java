package com.frauddetection.fraud;

import com.frauddetection.spark.SparkSessionFactory;
import com.frauddetection.utils.ConfigLoader;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;
public class FraudDetection {

    private static final String SILVER_PATH = ConfigLoader.get("s3.path.curated");
    private static final String FRAUD_ALERTS_PATH = ConfigLoader.get("s3.path.fraud.alerts");

    public static void main(String[] args) {

        SparkSession spark = SparkSessionFactory.create("Fraud Detector");

        Dataset<Row> transactions = spark.read()
                .format("delta")
                .load(SILVER_PATH)
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

//        highValues.show(5, false);

        // -----------------------------------------
        // RULE 2 — Card Testing (same user, same merchant, > 2 times)
        // -----------------------------------------
        System.out.println("Possible Card Testing Attack.........................");

        WindowSpec cardTestWindow = Window.partitionBy("user_id", "merchant");

        Dataset<Row> cardTesting = transactions
                .withColumn("txn_count", count("txn_id").over(cardTestWindow))
                .filter(col("txn_count").gt(2))
                .withColumn("fraud_flag", lit("CARD_TESTING"))
                .select("txn_id", "user_id", "amount", "txn_count", "merchant", "timestamp", "fraud_flag");

//        cardTesting.show(5, false);

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
                .select(
                        col("user_id"),
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("txn_count"),
                        col("total_amount"),
                        col("fraud_flag")
                )
                .orderBy(col("txn_count").desc());

//        velocity.show(5, false);

        System.out.println("========================= WRITING FRAUD ALERTS TO GOLD LAYER ==============================");

        // RULE 1 — High Value
        Dataset<Row> highValuesGold = highValues
                .withColumn("rule_detail", concat(lit("amount="), col("amount")))
                .withColumn("window_start", lit(null).cast(DataTypes.TimestampType))
                .withColumn("window_end", lit(null).cast(DataTypes.TimestampType))
                .select("txn_id", "user_id", "merchant", "timestamp",
                        "window_start", "window_end", "rule_detail", "fraud_flag");

// RULE 2 — Card Testing
        Dataset<Row> cardTestingGold = cardTesting
                .withColumn("rule_detail", concat(lit("txn_count="), col("txn_count")))
                .withColumn("window_start", lit(null).cast(DataTypes.TimestampType))
                .withColumn("window_end", lit(null).cast(DataTypes.TimestampType))
                .select("txn_id", "user_id", "merchant", "timestamp",
                        "window_start", "window_end", "rule_detail", "fraud_flag");

// RULE 3 — Velocity
        Dataset<Row> velocityGold = velocity
                .withColumn("txn_id", lit(null).cast(DataTypes.StringType))
                .withColumn("merchant", lit(null).cast(DataTypes.StringType))
                .withColumn("timestamp", lit(null).cast(DataTypes.TimestampType))
                .withColumn("rule_detail", concat(
                        lit("txn_count="), col("txn_count"),
                        lit(", total_amount="), col("total_amount")))
                .select("txn_id", "user_id", "merchant", "timestamp",
                        "window_start", "window_end", "rule_detail", "fraud_flag");

        Dataset<Row> allAlerts = highValuesGold
                .union(cardTestingGold)
                .union(velocityGold);

        allAlerts.write()
                        .format("delta")
                        .mode(SaveMode.Overwrite)
                        .save(FRAUD_ALERTS_PATH);

        System.out.println("Fraud alerts written to gold layer. Total alerts: " + allAlerts.count());

        SparkSessionFactory.stop();

    }
}

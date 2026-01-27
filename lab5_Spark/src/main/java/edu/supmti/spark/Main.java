package edu.supmti.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TP_Spark_SUPMTI")
                .getOrCreate();

        // Lecture du CSV depuis HDFS
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("hdfs://hadoop-master:9000/user/root/input/transaction_data.csv");

        // Affichage des données (Section III du TP)
        df.show(5);
        
        // Filtrage des transactions > 1000
        df.filter(col("Transaction Amount").gt(1000)).show();

        // Analyse par type de transaction
        df.groupBy("Transaction Type").sum("Transaction Amount").show();

        spark.stop();
    }
}
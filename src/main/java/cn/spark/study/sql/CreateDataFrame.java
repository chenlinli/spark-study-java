package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDataFrame {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CreateDataFrame")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().json("hdfs://spark1:9000/students.json");

        df.show();
    }
}

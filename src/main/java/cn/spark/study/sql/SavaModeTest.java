package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SavaModeTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("")
                .master("local")
                .getOrCreate();
        Dataset<Row> peopleDF = spark.read().format("json").load("hdfs://spark1:9000/people.json");
        peopleDF.write().format("json").mode(SaveMode.Append).save("hdfs://spark1:9000/people_savamode_test");
    }
}

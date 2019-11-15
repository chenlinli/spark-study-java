package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetPartitionDiscover {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetPartitionDiscover")
                .getOrCreate();
        //读取parquet的数据
        Dataset<Row> usersDF = spark.read().parquet("hdfs://spark1:9000/spark-study/users");
        usersDF.printSchema();
        usersDF.show();
    }
}

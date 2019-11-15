package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameOperation {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CreateDataFrame")
                .getOrCreate();
        //创建DataFrame，类似于一张表
        Dataset<Row> df = sparkSession.read().json("hdfs://spark1:9000/students.json");
        //打印df所有数据
        df.show();
        //打印元数据信息
        df.printSchema();
        //查询一列的值
        df.select("name").show();
        //查询某几列所有数据,并计算列
        df.select(df.col("name"),df.col("age").plus(1)).show();
        //对于某一列的值过滤
        df.filter(df.col("age").gt(18)).show();
        //根据一列分组，聚合
        df.groupBy(df.col("age")).count().show();

    }
}

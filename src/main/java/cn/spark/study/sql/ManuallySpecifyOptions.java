package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 收到配置数据源类型
 */
public class ManuallySpecifyOptions {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("")
                //.master("local")
                .getOrCreate();
        //读取文件为df
        Dataset<Row> peopleDF = spark.read().format("json")
               // .load("src/people.json");
                .load("hdfs://spark1:9000/people.json");
        peopleDF.select("name").write().format("parquet")
        .save("hdfs://spark1:9000/peopleName_java");

    }
}

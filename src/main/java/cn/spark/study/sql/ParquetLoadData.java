package cn.spark.study.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ParquetLoadData {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("")
                .getOrCreate();
        //读取parquet的数据
        Dataset<Row> usersDF = spark.read().parquet("hdfs://spark1:9000/users.parquet");
        //注册为临时表
        usersDF.registerTempTable("users");

        //查询数据
        Dataset<Row> nameDF = spark.sql("select name from users");
        //处理查询的数据
        JavaRDD<String> nameRDD = nameDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name：" + row.getString(0);
            }
        });
        List<String> collect = nameRDD.collect();
        for(String s:collect){
            System.out.println(s);
        }
    }
}

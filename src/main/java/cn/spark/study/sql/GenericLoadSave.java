package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenericLoadSave {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("")
                //.master("local")
                .getOrCreate();
        //读取文件为df
        Dataset<Row> usersDF = spark.read().load("hdfs://spark1:9000/users.parquet");
        usersDF.printSchema();
        /**
         * root
         |-- name: string (nullable = true)
         |-- favorite_color: string (nullable = true)
         |-- favorite_numbers: array (nullable = true)
         |    |-- element: integer (containsNull = true)
         */
        usersDF.show();
        /**
         * +------+--------------+----------------+
         |  name|favorite_color|favorite_numbers|
         +------+--------------+----------------+
         |Alyssa|          null|  [3, 9, 15, 20]|
         |   Ben|           red|              []|
         */
        //保存到文件

        usersDF.select("name", "favorite_color")
                .write().save("hdfs://spark1:9000/nameAndColors.parquet");
    }
}

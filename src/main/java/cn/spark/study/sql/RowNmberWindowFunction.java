package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class RowNmberWindowFunction {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RowNmberWindowFunction")
                .enableHiveSupport()
                .getOrCreate();

        //创建销售额表
        spark.sql("DROP TABLE IF EXISTS sales");
        spark.sql("CREATE TABLE IF NOT EXISTS sales(product STRING,category STRING,revenue BIGINT)");
        spark.sql("LOAD DATA LOCAL INPATH '/usr/local/spark-study/resources/sales.txt' INTO TABLE sales");
        //统计逻辑，开窗函数row_number()：每个分组内的数据，按照其排序给一个行号（从1开始
        // ）
        Dataset<Row> top3DF = spark.sql("" +
                "select product,category,revenue from (" +
                "SELECT product ,category,revenue,row_number() over (partition by category order by revenue desc) rank" +
                " from sales) tmp_sales"+
                " where rank <= 3"
        );

        //row_number() +over (partition by根据什么分组，可以order by组内排序)，然后row_number就可以给组内的每一行一个序号

        //每组排名前三的存入表
        spark.sql("DROP TABLE IF EXISTS top3_sales");
        top3DF.write().saveAsTable("top3_sales");
    }
}

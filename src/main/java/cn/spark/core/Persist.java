package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Persist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        //持久化
        //cache/persist使用有规则：必须在transformation/textFile后必须立即连续调用cache/persist才可以
        //如果创建RDD后，然后单独另起一行执行cache/persist没有用，而且报错，大量文件会丢失
        JavaRDD<String> lines = javaSparkContext.textFile("src/spark.txt")
                .cache()//立即调用
                ;

        long begin =System.currentTimeMillis();
        long count = lines.count();
        long end = System.currentTimeMillis();
        System.out.println(end-begin);

        begin =System.currentTimeMillis();
        count = lines.count();
        end = System.currentTimeMillis();
        System.out.println(end-begin);

        begin =System.currentTimeMillis();
        count = lines.count();
        end = System.currentTimeMillis();
        System.out.println(end-begin);

    }
}

package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class HiveDataSource {
    public static void main(String[] args) {
        SparkSession hiveContext = SparkSession.builder().appName("HiveDataSource")
                .enableHiveSupport()
                .getOrCreate();
        //使用hiveContext的sql/hql(),执行语句
        //student_infos
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
        //判断student_infos是否不存在，如果不存在，则创建该表
        hiveContext.sql("create table if not exists student_infos(name STRING,age INT) USING hive");
        //导入数据到表里
        hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/local/spark-study/student_infos.txt' INTO TABLE student_infos");

        //同样样方式
        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores(name STRING,score INT) USING hive");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/local/spark-study/student_scores.txt' INTO TABLE student_scores");

        //执行查询,关联两张表，成绩》=80
        //执行sql,返回df
        Dataset<Row> goodStudentDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss ON si.name=ss.name "
                + "WHERE ss.score>=80");

        //接着将df保存到表
        //df对应的RDD的元素是Row就可以，把DF-->hive表
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentDF.write().saveAsTable("good_student_infos");

        //good_student_infos表创建DF
        //hive表创建DF
        Dataset<Row> goodStuRowsDF = hiveContext.table("good_student_infos");
        List<Row> collect = goodStuRowsDF.javaRDD().collect();
        for(Row r:collect){
            System.out.println(r);
        }


    }
}

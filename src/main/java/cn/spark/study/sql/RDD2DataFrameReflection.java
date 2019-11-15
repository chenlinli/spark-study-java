package cn.spark.study.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * 反射转换rdd为dataframe
 */
public class RDD2DataFrameReflection {

    public static void main(String[] args) {
        //创建普通RDD
        SparkSession sparkSession = SparkSession.builder().appName("").master("local").getOrCreate();
        //rdd
        JavaRDD<String> lines = sparkSession.read()
                .textFile("src/students.txt").javaRDD();
        //
        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                String[] split = s.split(",");
                return new Student(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
            }
        });
        //反射将javaBean-->Df
        Dataset<Row> studentsDF = sparkSession.createDataFrame(students, Student.class);
        //拿到df就可以注册为临时表，然后对其中的数据执行SQL
        studentsDF.registerTempTable("students");
        //针对临时表，查询
        Dataset<Row> teenagerDF = sparkSession.sql("select * from students where age>17");
        //查询的df-->rdd
        JavaRDD<Row> teenagerRdd = teenagerDF.javaRDD();
        //rdd数据映射-->Student
        JavaRDD<Student> newStudendsRdd = teenagerRdd.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                //row里的是数据的顺序可能与bean里定义的顺序不一致，得根据df的schema决定
                int age = row.getInt(0);
                int id = row.getInt(1);
                String name = row.getString(2);
                return new Student(id,name,age);
            }
        });
        List<Student> collect = newStudendsRdd.collect();
        for (Student s:collect){
            System.out.println(s);
        }

    }


}


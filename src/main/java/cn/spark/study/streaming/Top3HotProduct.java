package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 与spark sql整合
 */
public class Top3HotProduct {
    public static void main(String[] args) throws InterruptedException {
       SparkConf conf = new SparkConf()
               .setAppName("Top3HotProduct")
               .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //log：leo iphone mobile_phone:username,productName,category
        //获取输入数据
        //spark streaming案例为什么基于socket:因为方便，企业常用的是kafka数据源
        JavaReceiverInputDStream<String> productClickLogsDStream = jsc.socketTextStream("spark1", 9999);

        //做一个映射，将每个种类的商品映射为 (category_product,1)
        //便于window操作,统计一个种类的某个商品的点击次数
        JavaPairDStream<String,Integer> categoryProductPairsDStream = productClickLogsDStream.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] split = s.split(" ");
                        return new Tuple2<>(split[2]+"_"+split[1],1); //category product,`
                    }
                }
        );

        //window操作
        //每隔10s计算最近60s数据的reduceByKey,计算出最近60s的商品点击次数
        JavaPairDStream<String,Integer> categoryProductCountsDStream =
                categoryProductPairsDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
        },Durations.seconds(60),Durations.seconds(10));

        //每隔10s会对当前60s内的12个RDD做一次计算
      categoryProductCountsDStream.foreachRDD((categoryProductCountsRDD,time)->{

          JavaRDD<ProductClickLog> categoryProductCountRowRDD =
                  categoryProductCountsRDD.map(categoryProductCount -> {
              String[] split = categoryProductCount._1.split("_");
              ProductClickLog productClickLog = new ProductClickLog(split[0], split[1], categoryProductCount._2);
              return productClickLog;
          });
          SparkSession spark = SparkSession.builder()
                  .config(categoryProductCountsRDD.context().getConf())
                  .enableHiveSupport()
                  .getOrCreate();

          Dataset<Row> categoryProductCountDF = spark.createDataFrame(categoryProductCountRowRDD, ProductClickLog.class);

          categoryProductCountDF.createOrReplaceTempView("product_click_log");

          Dataset<Row> categoryProductCountTop3 = spark.sql(
                  "select category,product,count from ("
                          + "select category,product,count, row_number() over (partition by category order by count desc) rank from product_click_log"
                          + ") tmp where rank <= 3");

            categoryProductCountTop3.show();
      });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}

//60s的点击次数,foreachRDD 统计热门商品
      /*  categoryProductCountsDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> catogoryProductCountsRDD) throws Exception {
                //catogoryProductCountRDD:一个窗口内的统计的商品点击次数
                //转为javaRDD<Row>
                JavaRDD<Row> categoryProductCountRowRdd = catogoryProductCountsRDD.map(
                        new Function<Tuple2<String, Integer>, Row>() {
                            @Override
                            public Row call(Tuple2<String, Integer> categoryProductCount) throws Exception {
                                String[] categoryProduct = categoryProductCount._1.split("_");
                                //每一行是:category product count
                                System.out.println("==============row rdd: "+categoryProduct[0]+" "+categoryProduct[1]+" "+categoryProductCount._2);
                                return RowFactory.create(categoryProduct[0],categoryProduct[1],categoryProductCount._2);
                            }
                        }
                );
                //row RDD-->DF
                List<StructField> structFields= Arrays.asList(
                        DataTypes.createStructField("category",DataTypes.StringType,true),
                        DataTypes.createStructField("product",DataTypes.StringType,true),
                        DataTypes.createStructField("count",DataTypes.IntegerType,true)
                );
                StructType structType = DataTypes.createStructType(structFields);
                Dataset<Row> categroryProductCountDF = spark.createDataFrame(categoryProductCountRowRdd, structType);

                //将60s的每个商品点击次数数据注册临时表
                categroryProductCountDF.registerTempTable("product_click_log");
                System.out.println("=========================start sql======================");

                //执行sql:查询每个分类的前3点击商品
                Dataset<Row> top3ProductDF = spark.sql(
                        "select category,product,count from ("
                                + "select category,product,count, row_number() over (partition by category order by count desc) rank from product_click_log"
                                + ") tmp where rank >= 3");
                System.out.println("============================end sql ======================");

                top3ProductDF.show(); //触发job,企业保存到db里，配合展示查询

            }
        });
*/


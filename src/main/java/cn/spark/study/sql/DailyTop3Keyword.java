package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class DailyTop3Keyword {
    public static void main(String[] args) {
        SparkSession hiveContext = SparkSession.builder().appName("DailyTop3Keyword")
                .enableHiveSupport().getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(hiveContext.sparkContext());
        //伪造数据:实际里可能是mysql的数据
        HashMap<String, List<String>> queryMap = new HashMap<>();
        queryMap.put("city", Arrays.asList("beijing"));
        queryMap.put("platform", Arrays.asList("android"));
        queryMap.put("version", Arrays.asList("1.0","1.2","1.5","2.0"));

        //查询条件广播变量
        final Broadcast<Map<String, List<String>>> queryMapBroadcast = sc.broadcast(queryMap);
        //hdfs-->rdd
        JavaRDD<String> rowRdd = sc.textFile("hdfs://spark1:9000/keyword.txt");

        //广播变量来过滤rdd
        JavaRDD<String> filteredRdd = rowRdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];
                //比对
                Map<String, List<String>> queruMap = queryMapBroadcast.value();
                List<String> cities = queruMap.get("city");
                if(cities.size()>0 && !cities.contains(city))
                    return false;
                List<String> platforms = queruMap.get("platform");
                if(platforms.size()>0 && !platforms.contains(platform))
                    return false;
                List<String> versions = queruMap.get("version");
                if(versions.size()>0 && !versions.contains(version)){
                    return false;
                }
                return true;
            }
        });

        //转换格式-->日期，搜索词，用户
        JavaPairRDD<String, String> dateKeywordUserRDD = filteredRdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                String date = logSplited[0];
                String user = logSplited[1];
                String keyword = logSplited[2];
                return new Tuple2<>(date+"_"+keyword,user);
            }
        });
        
        //分组，每天每个搜索词的用户
        JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
        
        //分组的用户去重，获得uv
        JavaPairRDD<String, Long> dateKeywordUVRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers) throws Exception {
                String dateKeyword = dateKeywordUsers._1;
                Iterator<String> users = dateKeywordUsers._2.iterator();
                //用户去重，统计个数
                HashSet<String> set = new HashSet<>();
                while (users.hasNext()){
                    set.add(users.next());
                }
                return new Tuple2<>(dateKeyword,(long)set.size());
            }
        });

        //uvrdd-->DF,便于取每日的前三
        JavaRDD<Row> dateKeywordUVRowRDD = dateKeywordUVRDD.map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> dateKeywordUV) throws Exception {
                String date = dateKeywordUV._1.split("_")[0];
                String keyword = dateKeywordUV._1.split("_")[1];
                long uv = dateKeywordUV._2;
                return RowFactory.create(date,keyword,uv);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> dateKeywordUCDF = hiveContext.createDataFrame(dateKeywordUVRowRDD, structType);

        //Df注册临时表，开窗函数排名
        dateKeywordUCDF.registerTempTable("daily_keyword_uv");
        Dataset<Row> dailyTop3KeywordDF = hiveContext.sql(
                "SELECT date,keyword,uv "
                        + "FROM ("
                            + "SELECT "
                            + "date,"
                            + "keyword,"
                            + "uv,"
                            + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
                            + "FROM daily_keyword_uv"
                        + ") tmp "
                        + "WHERE rank<=3");

        //rdd->rdd,计算每个top3词的搜索总数
        JavaRDD<Row> dateTop3KeywordRDD = dailyTop3KeywordDF.javaRDD();
        JavaPairRDD<String, String> top3DateKeywordRDD = dateTop3KeywordRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.get(0).toString(),row.get(1).toString()+"_"+row.get(2).toString());
            }
        });

        JavaPairRDD<String, Iterable<String>> top3DatekeywordsRDD = top3DateKeywordRDD.groupByKey();
        JavaPairRDD<Long,String> uvDateKeywordsRdd = top3DatekeywordsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> dateKeywordUV) throws Exception {
                String dateKeyword = dateKeywordUV._1;
                Long totalUV = 0L;
                Iterator<String> keywordUVs = dateKeywordUV._2.iterator();

                while (keywordUVs.hasNext()){
                    String keywordUV = keywordUVs.next();
                    long uv = Long.valueOf(keywordUV.split("_")[1]);
                    totalUV+=uv;
                    dateKeyword+=","+keywordUV;
                }
                return new Tuple2<>(totalUV,dateKeyword);
            }
        });

        //每天总搜索uv倒叙排序
        JavaPairRDD<Long, String> sortedUvDateKeywordsRdd = uvDateKeywordsRdd.sortByKey(false);

        //再次映射将排序后的数据--》Iterable<Row>
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRdd.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            @Override
            public Iterator<Row> call(Tuple2<Long, String> tuple) throws Exception {
                String dateKeywords = tuple._2;
                String[] split = dateKeywords.split(",");
                String date = split[0];
                List<Row> rows = new ArrayList<>();
                for(int i=1;i<split.length;i++) {
                    rows.add(RowFactory.create(date,
                            split[i].split("_")[0],
                            Long.valueOf(split[i].split("_")[1])));
                }
                return rows.iterator();
            }
        });

        Dataset<Row> finalDF = hiveContext.createDataFrame(sortedRowRDD, structType);
        finalDF.write().saveAsTable("daily_top3_keyword_uv");
        sc.close();

    }
}

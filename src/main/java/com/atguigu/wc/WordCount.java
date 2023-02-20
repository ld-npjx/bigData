package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment ev = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath="C:\\JavaCode\\Flink\\src\\main\\resources\\test.txt";
        DataSet<String> dataSource = ev.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> dataSet = dataSource.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        //0 , 1是元组位置
        dataSet.print();
    }
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] strings = str.split(" ");
           for (String s:strings){
               collector.collect(new Tuple2<>(s,1));
           }
        }
    }
}

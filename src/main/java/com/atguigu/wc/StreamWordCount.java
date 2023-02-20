package com.atguigu.wc;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment ev = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath="C:\\JavaCode\\Flink\\scr\\main\\resources\\test.txt";
        DataStream<String> dataSource = ev.readTextFile(inputPath);



    }

}

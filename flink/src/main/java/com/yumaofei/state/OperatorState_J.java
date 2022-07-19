package com.yumaofei.state;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorState_J {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\word.txt";
        DataStream<String> input = env.readTextFile(path);

//        input.flatMap()
    }

    /*class MyFlatmap implements FlatMapFunction<String,String>, ListCheckpointed<Integer>{
        @Override
        public String flatMap(){

        }
    }*/

}

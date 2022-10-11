package com.yumaofei;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Kafka2Kudu {

    public static void main(String[] args) throws Exception{

        //读取配置文件
//        String config_path=args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(new FileInputStream(new File("D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flinkKafkaKudu\\src\\main\\resources\\kafka.properties")))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置文件放入流式环境
        env.getConfig().setGlobalJobParameters(parameterTool);

        //配置kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", parameterTool.get("kafka.ips"));
        props.setProperty("group.id", parameterTool.get("kafka.group.name"));
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(parameterTool.get("kafka.topic.name"), new SimpleStringSchema(), props);
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromGroupOffsets();

        DataStream<String> stream = env.addSource(consumer);

        stream.print();

//        stream.addSink(new KuduSink()).name("kafkaTokudu_test");

        env.execute();

    }

    private static Map<String, String> getenv()
    {
        Map<String, String> map = new HashMap();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

}



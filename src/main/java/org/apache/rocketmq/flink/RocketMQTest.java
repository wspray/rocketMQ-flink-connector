package org.apache.rocketmq.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.rocketmq.flink.source.RocketMQSource;
import org.apache.rocketmq.flink.source.reader.deserializer.RocketMQValueOnlyDeserializationSchemaWrapper;
import org.apache.rocketmq.flink.source.reader.deserializer.SimpleStringSchema;

public class RocketMQTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        RocketMQSource<String> source =
                RocketMQSource.<String>builder()
                        .setNameServerAddress("10.130.16.166:9876")
                        .setTopic("T1")
                        .setConsumerGroup("G1")
                        .setTag("tag")
                        .setStartFromTimeStamp(1753132059000L)
                        .setStopInMs(1753140898895L)
                        .setDeserializer(
                                new RocketMQValueOnlyDeserializationSchemaWrapper<>(
                                        new SimpleStringSchema()))
                        .build();

        DataStreamSource<String> newSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "new source")
                        .setParallelism(4);

        newSource.print().setParallelism(1);

        env.execute();
    }
}

package com;

import com.utils.DayHourPathBucketer;
import com.utils.ParquetSinkWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class KafkaSink2HDFSasParquet {

    public static String brockers = "localhost:9092";
    
    public static String baseDir = "hdfs://locakhost:9000/flink";
    
    public static String topic = "mytopic";

   
    public static String schema = "{\"name\" :\"UserGroup\"" +
            ",\"type\" :\"record\"" +
            ",\"fields\" :" +
            "     [ {\"name\" :\"data\",\"type\" :\"string\" }] " +
            "} ";

    public static GenericData.Record transformData(String msg) {
        GenericData.Record record = new GenericData.Record(Schema.parse(schema));
        record.put("data", msg);
        return record;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20 * 60 * 1000);//20分钟
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);//至少一次就可以了，业务逻辑不需要EXACTLY_ONCE
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brockers);
        props.setProperty("group.id", "flink.xxx.xxx");


        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>(topic,
                new SimpleStringSchema(), props);
        consumer.setStartFromLatest();
        DataStreamSource<String> stream = env.addSource(consumer);

        SingleOutputStreamOperator<GenericData.Record> output = stream.map(KafkaSink2HDFSasParquet::transformData);


        BucketingSink<GenericData.Record> sink = new BucketingSink<GenericData.Record>(baseDir);
        sink.setBucketer(new DayHourPathBucketer());
        sink.setBatchSize(1024 * 1024 * 400); //400M
        ParquetSinkWriter writer = new ParquetSinkWriter<GenericData.Record>(schema);
        sink.setWriter(writer);


        output.addSink(sink);
        env.execute("KafkaSink2HDFSasParquet");
    }
}
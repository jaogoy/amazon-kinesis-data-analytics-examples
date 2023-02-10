package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * A basic Kinesis Data Analytics for Java application with Kinesis data
 * streams as source and sink.
 */
public class BasicStreamingJob {
    private static final Log log = LogFactory.getLog(BasicStreamingJob.class);

    private static final String region = "us-west-2";
    private static final String inputStreamName = "ExampleInputStream";
    private static final String outputStreamName = "ExampleOutputStream";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static DataStream<String> createSourceFromApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties inputProperties = applicationProperties.get("ConsumerConfigProperties");
        if (inputProperties == null) {
            inputProperties = new Properties();
        }
        if (!inputProperties.contains(ConsumerConfigConstants.AWS_REGION)) {
            inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        }
        if (!inputProperties.contains(ConsumerConfigConstants.STREAM_INITIAL_POSITION)) {
            inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        }

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        outputProperties.setProperty("AggregationEnabled", "false");

        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    private static FlinkKinesisProducer<String> createSinkFromApplicationProperties() throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(),
                applicationProperties.get("ProducerConfigProperties"));

        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* if you would like to use runtime configuration properties, uncomment the lines below
         * DataStream<String> input = createSourceFromApplicationProperties(env);
         */
        log.info("Create an input");
        // DataStream<String> input = createSourceFromStaticConfig(env);
        DataStream<String> input = createSourceFromApplicationProperties(env);

        /* if you would like to use runtime configuration properties, uncomment the lines below
         * input.addSink(createSinkFromApplicationProperties())
         */
        log.info("Start to create an sink");
        // input.addSink(createSinkFromStaticConfig());
        input.addSink(createCDSinkFromApplicationProperties());
        log.info("Success to add a sink");

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static SinkFunction<String> createCDSinkFromApplicationProperties() throws IOException {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties outputProperties = applicationProperties.get("ProducerConfigProperties");
        if (outputProperties == null) {
            outputProperties = new Properties();
            log.info("ProducerConfigProperties is not set. It will use default config");
        }

        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", outputProperties.getProperty("jdbc-url", "jdbc:mysql://172.31.86.199:9030"))
                .withProperty("load-url", outputProperties.getProperty("load-url", "172.31.86.199:8030"))
                .withProperty("username", outputProperties.getProperty("username", "admin"))
                .withProperty("password", outputProperties.getProperty("password", "yuzhuo"))
                .withProperty("table-name", outputProperties.getProperty("table-name", "users"))
                .withProperty("database-name", outputProperties.getProperty("database-name", "lilytest"))
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.jsonpaths", "[\"event_time\", \"ticker\", \"price\"]")
                // .withProperty("sink.properties.columns", "value, id=101")
                .withProperty("sink.properties.strip_outer_array", "true");

        log.info("properties: " + outputProperties.toString());
        for (Map.Entry<Object, Object> property : outputProperties.entrySet()) {
            if (StringUtils.startsWith(property.getKey().toString(), "sink.")) {
                builder.withProperty(property.getKey().toString(), property.getValue().toString());
            }
        }

        log.info("create a cd Sink.");
        return StarRocksSink.sink(builder.build());
    }
}

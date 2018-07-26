package com.example.bigdata;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class KafkaToBeamStreamingProcessor {

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    static final String BOOTSTRAP_SERVERS = "xxxxxx:9092";  // Default bootstrap Kafka servers
    static final String TEMP_LOCATION = "gs://xxx/libs/";  // Default output path
    static final String OUTPUT_PATH = "gs://xxx/outputs/";  // Default output path
    static final String TOPIC = "mytopic";  // Default Kafka topic name
    static final int WINDOW_SIZE = 60;  // Default window duration in seconds
    static final int NUM_SHARDS = 1;  // Default number of shards

    private enum OutputResultTarget {
        PATH, GOOGLE_HANGOUTS
    }

    static Options options = null; // Options that configure the Dataflow pipeline

    /**
     * Specific pipeline options.
     */
    private interface Options extends DataflowPipelineOptions {
        @Description("Kafka bootstrap servers")
        @Default.String(BOOTSTRAP_SERVERS)
        String getBootstrap();
        void setBootstrap(String value);

        @Description("Temporary Location Path")
        @Default.String(TEMP_LOCATION)
        String getTemporaryLocation();
        void setTemporaryLocation(String value);

        @Description("Output Path")
        @Default.String(OUTPUT_PATH)
        String getOutput();
        void setOutput(String value);

        @Description("Kafka topic name")
        @Default.String(TOPIC)
        String getTopic();
        void setTopic(String value);

        @Description("Fixed window duration, in seconds")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Fixed number of shards to produce per window, or null for runner-chosen sharding")
        @Default.Integer(NUM_SHARDS)
        Integer getNumShards();
        void setNumShards(Integer numShards);
    }

    public final static void main(String args[]) throws Exception {
        options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
        options.setRunner(DataflowRunner.class);
        options.setJobName("kafka-to-beam");
        options.setTempLocation(options.getTemporaryLocation());
        options.setProject("kubernetes-shenanigans");
        options.setGcpTempLocation(options.getTemporaryLocation());
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline
                .apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata())
                .apply(Values.<String>create());

        PCollection<String> windowsedTweets =
                input.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowSize()))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes()
                );

        PCollection<KV<String, Long>> languageCounts =
                windowsedTweets
                        .apply(new LanguageCount.CountLanguage())
                        .apply(Sum.longsPerKey());

        languageCounts
                .apply(MapElements.via(new LanguageCount.FormatAsTextFn()));

        OutputResults(OutputResultTarget.PATH, languageCounts);

        PipelineResult result = pipeline.run();
        try{
            result.waitUntilFinish();
        } catch (Exception e){
            result.cancel();
        }
    }

    private static void OutputResults(OutputResultTarget targetType, PCollection input){

        switch (targetType){
            case GOOGLE_HANGOUTS:
                throw new NotImplementedException();
            default:
            case PATH:
                input.apply(TextIO.write()
                        .to(options.getOutput())
                        .withSuffix(".txt")
                        .withWindowedWrites()
                        .withNumShards(options.getNumShards()));
                break;
        }
    }
}

package com.example.bigdata;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaToBeamProcessor {

    /**
     * Specific pipeline options.
     */
    private interface Options extends DataflowPipelineOptions {

        @Description("Project name")
        String getProjectName();
        void setProjectName(String value);

        @Description("Beam job name")
        String getBeamJobName();
        void setBeamJobName(String value);

        @Description("Kafka bootstrap servers")
        String getBootstrap();
        void setBootstrap(String value);

        @Description("Temporary Location Path")
        String getTemporaryLocation();
        void setTemporaryLocation(String value);

        @Description("Output Path")
        String getOutputPath();
        void setOutputPath(String value);

        @Description("Kafka output topic name")
        String getOutputTopic();
        void setOutputTopic(String value);

        @Description("Kafka input topic name")
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Fixed window duration, in seconds")
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Fixed number of shards to produce per window, or null for runner-chosen sharding")
        Integer getNumShards();
        void setNumShards(Integer numShards);
    }

    public final static void main(String args[]) throws Exception {

        Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

        File configFile = new File("config.properties");

        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);

            options.setProjectName(props.getProperty("project_name"));
            options.setBeamJobName(props.getProperty("beam_job_name"));
            options.setBootstrap(props.getProperty("bootstrap_servers"));
            options.setTemporaryLocation(props.getProperty("temp_location"));
            options.setOutputPath(props.getProperty("output_path"));
            options.setOutputTopic(props.getProperty("output_topic"));
            options.setInputTopic(props.getProperty("input_topic"));
            options.setWindowSize(Integer.parseInt(props.getProperty("window_size")));
            options.setNumShards(Integer.parseInt(props.getProperty("num_shards")));

            reader.close();
        } catch (FileNotFoundException ex) {
            // file does not exist
        } catch (IOException ex) {
            // I/O error
        }

        // Configure the options for Beam pipeline and create it
        options.setRunner(DataflowRunner.class);
        options.setProject(options.getProjectName());
        options.setJobName(options.getBeamJobName());
        options.setTempLocation(options.getTemporaryLocation());
        options.setGcpTempLocation(options.getTemporaryLocation());
        Pipeline pipeline = Pipeline.create(options);

        // Construct pipeline
        PCollection<String> input = pipeline
                .apply("Read from Kafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getInputTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata())
                .apply("Create values", Values.create());

        PCollection<String> windowedMessages =
                input.apply("Apply windows", Window.<String>into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowSize()))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes()
                );

        PCollection<String> batchedResults =
                windowedMessages
                .apply("Transform messages", ParDo.of(new MessageLanguageHelper.AssignKeys()))
                        .apply("Batch messages", GroupByKey.create())
                        .apply("Detect and count languages - batched", ParDo.of(new MessageLanguageHelper.ExtractBatchedLanguageFn()));

        batchedResults
                .apply("Write to Kafka", KafkaIO.<Void, String>write()
                .withBootstrapServers(options.getBootstrap())
                .withTopic(options.getOutputTopic())
                .withValueSerializer(StringSerializer.class)
                .values()
        );

        PCollection<String> streamingResults = windowedMessages
                .apply("Detect languages", ParDo.of(new MessageLanguageHelper.ExtractStreamingLanguageFn()))
                .apply("Count languages - streaming", Count.perElement())
                .apply("Reduce by keys", Sum.longsPerKey())
                .apply("Format results", MapElements.via(new MessageLanguageHelper.FormatAsTextFn()));


        streamingResults
                .apply("Write to file", TextIO.write()
                .to(options.getOutputPath())
                .withSuffix(".txt")
                .withWindowedWrites()
                .withNumShards(options.getNumShards()));

        PipelineResult result = pipeline.run();

        try{
            result.waitUntilFinish();
        } catch (Exception e){
            result.cancel();
        }
    }
}

# Apache Kafka to Apache Beam, Languages Detection

This project enables streaming messages from [Kafka](http://kafka.apache.org/) to [Beam](https://beam.apache.org/) and apply
languages detection on those messages.

# Environment configurations and validations

Open `config.properties` and edit the values using text editor or IDE.

# Step-by-step

The KafkaToBeamProcessor will deploy the Apache Beam pipeline that performs the following steps:
- 1 Keep consuming messages from Kafka;<br />
- 2 Apply windowing on streaming messages;<br />
- 3 Detect messages' language and generate results in 2 ways, in parallel:<br />
    - 3.1 Batching<br />
        - 3.1.1 Batch windowed messages into 1 trunk and send it to [Google Translate API](https://cloud.google.com/translate/docs/);<br />
        - 3.1.2 Get back a list of language detections;<br />
        - 3.1.3 Publish the results to Kafka<br />
    - 3.2 Streaming<br />
        - 3.2.1 Stream each message to Google Translate API;<br />
        - 3.2.2 Get back a single language detection for that message;<br />
        - 3.2.3 Output the results to a file, with window<br />

To kickoff the process, simply run `KafkaToBeamProcessor.main()`.
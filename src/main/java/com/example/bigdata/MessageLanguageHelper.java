package com.example.bigdata;

import com.google.cloud.translate.Detection;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageLanguageHelper {

    /**
     * A SimpleFunction that call Google Translate API to get a list of languages of the input.
     * For batched.
     */
    static class ExtractBatchedLanguageFn extends DoFn<KV<Long, Iterable<String>>, String> {
        private final Counter emptyLines = Metrics.counter(ExtractBatchedLanguageFn.class, "emptyLines");

        @ProcessElement
        public void processElement(@Element KV<Long, Iterable<String>> input, OutputReceiver<String> receiver) {
            ArrayList<String> elements = Lists.newArrayList(input.getValue());

            if (elements == null || elements.size() == 0) {
                emptyLines.inc();
                return;
            }
            Translate translate = CreateTranslateService();
            List<Detection> detections = translate.detect(elements);

            List<String> languages = detections.stream().map(Detection::getLanguage).collect(Collectors.toList());
            Map<String, Long> mapLanguageOccurrences = languages.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, Long> entry : mapLanguageOccurrences.entrySet()) {
                builder.append(String.format("%s: %s%n", entry.getKey(), entry.getValue()));
            }
            receiver.output(builder.toString());
        }
    }

    /**
     * A SimpleFunction that call Google Translate API to get the language of the input.
     * For Streaming.
     */
    static class ExtractStreamingLanguageFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractStreamingLanguageFn.class, "emptyLines");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            if (element == null || element.isEmpty()) {
                emptyLines.inc();
                return;
            }

            Translate translate = CreateTranslateService();
            Detection detection = translate.detect(element);
            receiver.output(detection.getLanguage());
        }
    }

    /**
     * Assign an identical keys to all the messages' kv pairs
     */
    public static class AssignKeys extends DoFn<String, KV<Long, String>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            Long setKey = 0L;
            c.output(KV.of(setKey, c.element()));
        }
    }

    /**
     * A SimpleFunction that converts the Language and its Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return String.format("%s: %s", input.getKey(), input.getValue());
        }
    }

    /**
     * Create Google Translate API Service.
     *
     * @return Google Translate Service
     */
    private static Translate CreateTranslateService() {
        return TranslateOptions.newBuilder().build().getService();
    }
}
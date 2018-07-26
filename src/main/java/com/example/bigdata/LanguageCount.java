package com.example.bigdata;

import com.google.cloud.translate.Detection;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class LanguageCount {

    static class ExtractLanguageFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractLanguageFn.class, "emptyLines");

        /** A SimpleFunction that call Google Translate API to get the language of the input. */
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

    /** A SimpleFunction that converts the Language and its Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
                return String.format("%s: %s", input.getKey(), input.getValue());
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted language counts.
     */
    public static class CountLanguage extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Translate messages into individual languages.
            PCollection<String> languages = lines.apply(
                    ParDo.of(new ExtractLanguageFn()));

            // Count the number of times each language occurs.
            PCollection<KV<String, Long>> languageCounts =
                    languages.apply(Count.perElement());

            return languageCounts;
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
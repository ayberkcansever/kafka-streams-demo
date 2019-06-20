package com.canseverayberk.kafkastreamsdemo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Properties;

@Component
public class TimedWindowedWordCounter {

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/1");

        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> textLines = builder.stream("word_input", Consumed.with(stringSerde, stringSerde));
        KTable<Windowed<String>, Long> wordCounts = textLines
                .flatMapValues(value -> {
                    System.out.println(value);
                    return Arrays.asList(value.toLowerCase().split("\\W+"));
                })
                .groupBy((key, value) -> value)
                .windowedBy(TimeWindows.of(60000).advanceBy(10000))
                .count(Materialized.as("windowed_count"));

        wordCounts.toStream((wk, v) -> wk.key()).to("word_output", Produced.with(stringSerde, longSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        try {
            streams.start();
        } catch (final Throwable e) {
            e.printStackTrace();
        }
    }

}

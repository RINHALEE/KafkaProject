package org.woorin.kafka.processing;

import org.woorin.kafka.processing.JsonUtils.JsonSerde;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import java.util.Properties;

@Component
public class StreamBranchingByPrice {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    public KafkaStreams createStreams() {

        StreamsBuilder builder = new StreamsBuilder();
        JsonSerde jsonSerde = new JsonSerde();

        KStream<String, JsonNode> sourceStream = builder.stream("web-log-topic", Consumed.with(Serdes.String(), jsonSerde));

        // 분기 조건 정의 (total_price : 30000 이상, 미만)
        Predicate<String, JsonNode> isPriceAbove30000 = (key, value) -> value.get("total_price").asLong() >= 30000;
        Predicate<String, JsonNode> isPriceBelow30000 = (key, value) -> value.get("total_price").asLong() < 30000;

        // 분기 처리
        KStream<String, JsonNode>[] branches = sourceStream.branch(isPriceAbove30000, isPriceBelow30000);

        // 30000원 이상일 경우
        branches[0].mapValues(JsonUtils::transformToSchemaFormat)
                .to("above-30000-topic");

        // 30000원 미만인 경우
        branches[1].mapValues(JsonUtils::transformToSchemaFormat)
                .to("below-30000-topic");

        // Kafka Streams 구성
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-branching-by-price-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // 키 Serde 설정
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName()); // 값 Serde 설정


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return new KafkaStreams(builder.build(), props);
    }

}

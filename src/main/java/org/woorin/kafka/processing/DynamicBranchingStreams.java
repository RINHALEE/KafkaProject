package org.woorin.kafka.processing;

import org.woorin.kafka.support.JsonUtils;
import org.woorin.kafka.support.JsonUtils.JsonSerde;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import java.util.Properties;

@Component
public class DynamicBranchingStreams {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    public KafkaStreams createBranchStreams(String criterion, String price) {
        String input_topic = "web-log-topic";

        StreamsBuilder builder = new StreamsBuilder();
        JsonSerde jsonSerde = new JsonSerde();

        KStream<String, JsonNode> sourceStream = builder.stream(input_topic, Consumed.with(Serdes.String(), jsonSerde));

        // 데이터 값이 제대로 들어오는지 확인 -- 마지막에 삭제
        sourceStream.peek((key, value) -> {
                System.out.println("data : " + value);
        });

        if(criterion.equals("price")) {
            // 분기 조건 정의 (total_price : price값 이상, 미만)
            Predicate<String, JsonNode> isPriceAbove = (key, value) -> value.get("total_price").asLong() >= Long.parseLong(price);
            Predicate<String, JsonNode> isPriceBelow = (key, value) -> value.get("total_price").asLong() < Long.parseLong(price);

            // 분기 처리
            KStream<String, JsonNode>[] branches = sourceStream.branch(isPriceAbove, isPriceBelow);

            // price 이상일 경우
            branches[0].mapValues(JsonUtils::transformToSchemaFormat)
                    .to("above-"+price+"-topic");

            // price 미만인 경우
            branches[1].mapValues(JsonUtils::transformToSchemaFormat)
                    .to("below-"+price+"-topic");
        } else if (criterion.equals("gender")) {
            // 분기 조건 정의 (성별)
            Predicate<String, JsonNode> Female = (key, value) -> value.get("gender").asText().equals("F");
            Predicate<String, JsonNode> Male = (key, value) -> value.get("gender").asText().equals("M");

            // 분기 처리
            KStream<String, JsonNode>[] branches = sourceStream.branch(Female, Male);

            // 여성일 경우
            branches[0].mapValues(JsonUtils::transformToSchemaFormat)
                    .to("female-topic");

            // 남성일 경우
            branches[1].mapValues(JsonUtils::transformToSchemaFormat)
                    .to("male-topic");
        }

        // APPLICATION_CONFIG_ID 설정 - 분기처리 조건에 따라 다른 application.id를 사용하여 서로 다른 분기 처리 조건에 대해 독립적으로 Kafka Streams 애플리케이션을 운영
        String applicationId = "branch-app-" + System.currentTimeMillis();

        // Kafka Streams 구성
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // 키 Serde 설정
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName()); // 값 Serde 설정
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

}

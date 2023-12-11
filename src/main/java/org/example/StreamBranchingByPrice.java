package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@Component
public class StreamBranchingByPrice {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    public KafkaStreams createStreams() {

        StreamsBuilder builder = new StreamsBuilder();
        Serde<JsonNode> jsonSerde = new JsonSerde();

        KStream<String, JsonNode> sourceStream = builder.stream("web-log-topic", Consumed.with(Serdes.String(), jsonSerde));

        // 분기 조건 정의 (total_price : 30000 이상, 미만)
        Predicate<String, JsonNode> isPriceAbove30000 = (key, value) -> value.get("total_price").asLong() >= 30000;
        Predicate<String, JsonNode> isPriceBelow30000 = (key, value) -> value.get("total_price").asLong() < 30000;

        // 분기 처리
        KStream<String, JsonNode>[] branches = sourceStream.branch(isPriceAbove30000, isPriceBelow30000);

        // 30000원 이상일 경우
        branches[0].mapValues(value -> transformToSchemaFormat(value))
                .to("above-30000-topic");

        // 30000원 미만인 경우
        branches[1].mapValues(value -> transformToSchemaFormat(value))
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

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 데이터를 스키마 형식으로 변환하는 메소드
    private static JsonNode transformToSchemaFormat(JsonNode value) {
        ObjectNode newNode = objectMapper.createObjectNode();
        ObjectNode schemaNode = objectMapper.createObjectNode();
        ArrayNode fieldsArray = objectMapper.createArrayNode();

        // 스키마 정의
        schemaNode.put("type", "struct");
        schemaNode.set("fields", fieldsArray);

        // 로그 데이터의 필드 이름 동적 추출
        Iterator<Map.Entry<String, JsonNode>> fields = value.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            fieldsArray.add(createFieldNode(field.getValue().getNodeType().toString().toLowerCase(), field.getKey()));
        }

        schemaNode.put("optional", false);
        schemaNode.put("name", "log_data");

        // 최종 노드 구성
        newNode.set("schema", schemaNode);
        newNode.set("payload", value);

        return newNode;
    }

    private static ObjectNode createFieldNode(String type, String fieldName) {
        ObjectNode fieldNode = objectMapper.createObjectNode();
        fieldNode.put("type", type);
        fieldNode.put("optional", true);
        fieldNode.put("field", fieldName);
        return fieldNode;
    }

    public static class JsonSerde implements Serde<JsonNode> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Serializer<JsonNode> serializer() {
            return (topic, data) -> {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("JSON serialization failed", e);
                }
            };
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return (topic, data) -> {
                try {
                    return objectMapper.readTree(data);
                } catch (Exception e) {
                    throw new RuntimeException("JSON deserialization failed", e);
                }
            };
        }
    }
}

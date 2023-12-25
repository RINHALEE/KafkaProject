package org.woorin.kafka.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.springframework.stereotype.Component;
import org.woorin.kafka.support.JsonUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@Component
public class UserActivityExitDetector {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsExit.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;
    private static class ExitTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {
        private ProcessorContext context;
        private KeyValueStore<String, JsonNode> uidStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.uidStore = context.getStateStore("uidStore");
            // 정기적인 검사를 위한 스케줄 설정
            context.schedule(Duration.ofSeconds(100), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        }

        // 상태 저장소를 순회하며 이탈한 사용자를 감지
        private void punctuate(long timestamp) {
            List<String> toDelete = new ArrayList<>(); // 삭제할 키 목록 (이탈 감지된 키)
            List<KeyValue<String, JsonNode>> toForward = new ArrayList<>();

            // 순회하면서 처리할 항목을 임시 리스트에 추가
            uidStore.all().forEachRemaining(entry -> {
                long evtTimeMillis = extractEventTime(String.valueOf(entry.value));
                System.out.println("uidSore 순회 ");

                // evtTime을 LocalDateTime으로 변환
                LocalDateTime evtTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(evtTimeMillis), ZoneId.systemDefault());

                // 사용자의 마지막 활동으로부터 100초가 지난 경우, 해당 사용자는 이탈한 것으로 간주
                if (evtTime != null && Duration.between(evtTime, LocalDateTime.now()).getSeconds() > 100) {
                    toForward.add(KeyValue.pair(entry.key, entry.value));
                    toDelete.add(entry.key); // 삭제할 키 목록에 추가
                }
            });

            // 이탈 조건을 만족하는 데이터 전송 및 상태 저장소에서 삭제
            toForward.forEach(kv -> context.forward(kv.key, kv.value));
            toDelete.forEach(uidStore::delete);
        }

        // 데이터 변환 메서드: 스트림의 각 레코드에 대해 호출됨
        @Override
        public KeyValue<String, JsonNode> transform(String key, JsonNode value) {
            uidStore.put(key, value); // 상태 저장소에 데이터 저장
            return null; // 변환은 punctuate에서 수행하기 때문에 null 반환
        }

        // Transformer 종료 시 호출
        @Override
        public void close() {
        }
    }

    public KafkaStreams createExitStreams() {
        String input_topic = "web-log-topic";
        JsonUtils.JsonSerde jsonSerde = new JsonUtils.JsonSerde();

        StreamsBuilder builder = new StreamsBuilder();

        // 입력 토픽으로부터 데이터 스트림 생성
        KStream<String, JsonNode> stream = builder.stream(input_topic, Consumed.with(Serdes.String(), jsonSerde));

        // 스트림에서 UID 추출 및 상태 저장소에 데이터 저장
        KTable<String, JsonNode> uidTable = stream
                .selectKey((key, value) -> extractUid(String.valueOf(value)))
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .reduce((oldValue, newValue) -> newValue);

        // 상태 저장소 구성
        StoreBuilder<KeyValueStore<String, JsonNode>> uidStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("uidStore"),
                Serdes.String(),
                jsonSerde);
        builder.addStateStore(uidStoreBuilder);

        // 스트림에 Transformer 적용 및 결과 데이터를 출력 토픽으로 전송
        uidTable.toStream()
                .transform(ExitTransformer::new, "uidStore")
                .mapValues(JsonUtils::transformToSchemaFormat)
                .to("exit-event-topic", Produced.with(Serdes.String(), jsonSerde));

        // Kafka Streams 설정: 독립적인 애플리케이션 ID 생성 및 기타 설정
        String applicationId = "exit-app-" + System.currentTimeMillis();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    // UID 추출 함수: JSON 데이터에서 UID 추출
    private static String extractUid(String json) {
        try {
            JsonNode node = new ObjectMapper().readTree(json);
            return node.get("Uid").asText();
        } catch (IOException e) {
            return null;
        }
    }

    // 이벤트 시간 추출 및 파싱 함수: JSON 데이터에서 이벤트 시간 추출 및 변환
    private static long extractEventTime(String s) {
        try {
            JsonNode node = new ObjectMapper().readTree(s);
            String evtTimeString = node.get("evt_time").asText();
            LocalDateTime dateTime = LocalDateTime.parse(evtTimeString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH));
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {

            log.error("Error parsing evt_time", e);
            return 0L;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
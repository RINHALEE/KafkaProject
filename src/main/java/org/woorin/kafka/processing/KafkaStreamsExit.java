package org.woorin.kafka.processing;

import com.fasterxml.jackson.databind.JsonNode;
import org.woorin.kafka.processing.JsonUtils.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

@Component
public class KafkaStreamsExit {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsExit.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    public KafkaStreams createExitStreams() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<JsonNode> jsonSerde = new JsonSerde();

        KStream<String, JsonNode> inputStream = builder.stream("web-log-topic", Consumed.with(Serdes.String(), jsonSerde));

        String outputTopic = "exit-event-topic";

        // 이탈 감지 조건 정의
        Predicate<String, JsonNode> deviationCondition = (key, value) -> {
            long evtTime = parseEventTime(value.get("evt_time").asText());
            long currentTimestamp = System.currentTimeMillis();
            long deviation = currentTimestamp - evtTime;
            return deviation >= 100000; // 100초 이상 이탈 시 이탈로 처리
        };

        KStream<String, JsonNode> modifiedStream = inputStream
                .filter(deviationCondition)
                .selectKey((key, value) -> value.get("Uid").asText());

        // 새로운 키로 변경된 스트림을 outputTopic으로 전송
        modifiedStream
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde)) //Uid를 기준으로 그룹화
                .reduce((value1, value2) -> {
                    // 여러 이벤트 중에서 최근 이벤트 선택 (evt_time을 기준으로 최근 이벤트 선택)
                    long time1 = value1.get("evt_time").asLong();
                    long time2 = value2.get("evt_time").asLong();
                    return (time1 > time2) ? value1 : value2;
                });
        modifiedStream
                .filter(deviationCondition).mapValues(JsonUtils::transformToSchemaFormat)
                .to(outputTopic);

        // Kafka Streams 구성
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exit-event-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 애플리케이션 종료 시 스트림 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    private long parseEventTime(String evtTimeString) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);
            Date parsedDate = format.parse(evtTimeString);
            return parsedDate.getTime();
        } catch (ParseException e) {
            log.error("Error parsing evt_time", e);
            return 0L;
        }
    }
}


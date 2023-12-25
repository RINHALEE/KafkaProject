package org.woorin.kafka.support;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


@Service
public class KafkaConnectorService {
    @Value("${kafka.connect.url}")
    private String kafkaConnectUrl;

    @Value("${connection.url}")
    private String connectionUrl;

    @Value("${connection.user}")
    private String connectionUser;

    @Value("${connection.password}")
    private String connectionPassword;

    public boolean sendConnectorConfigToKafkaConnect(String connectorConfig) {
        // HTTP 헤더 설정
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // HTTP 요청 설정
        HttpEntity<String> requestEntity = new HttpEntity<>(connectorConfig, headers);

        // RestTemplate을 사용하여 Kafka Connect에 POST 요청 전송
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(
                kafkaConnectUrl,
                requestEntity,
                String.class
        );

        // HTTP 응답 상태 코드가 2xx인 경우 성공으로 간주
        return responseEntity.getStatusCode().is2xxSuccessful();
    }

    public String generateSinkConnectorConfig(String connectorName, String topicsRegex) {
        // Connector 구성을 생성하는 로직
        return String.format("""
                {
                    "name": "%s",
                    "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                        "connection.url": "%s",
                        "connection.user": "%s",
                        "connection.password": "%s",
                        "auto.create": "true",
                        "auto.evolve": "true",
                        "delete.enabled": "false",
                        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter.schemas.enable": "true",
                        "tasks.max": "1",
                        "pk.mode": "none",
                        "topics.regex": "%s"
                    }
                }
                """, connectorName,connectionUrl,connectionUser,connectionPassword,topicsRegex);
    }
    public boolean deleteConnectorFromKafkaConnect(String connectorName) {
        // Kafka Connect REST API 엔드포인트 URL
        String fullUrl = kafkaConnectUrl + "/" + connectorName;

        // RestTemplate을 사용하여 Kafka Connect에 DELETE 요청 전송
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> responseEntity = restTemplate.exchange(
                fullUrl,
                HttpMethod.DELETE,
                null,
                String.class
        );

        // HTTP 응답 상태 코드가 2xx인 경우 성공으로 간주
        return responseEntity.getStatusCode().is2xxSuccessful();
    }
}

package org.woorin.kafka.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@RestController
public class KafkaConnectorController {

    // Kafka Connect REST API 엔드포인트 URL
    @Value("${kafka.connect.url}")
    private String kafkaConnectUrl;

    @Value("${connection.url}")
    private String connectionUrl;

    @Value("${connection.user}")
    private String connectionUser;

    @Value("${connection.password}")
    private String connectionPassword;

    @PostMapping("/connectors")
    public ResponseEntity<String> createConnector(@RequestBody Map<String, String> requestBody) {
        String connectorName = requestBody.get("name");
        String topicsRegex = requestBody.get("topics.regex");

        if (connectorName == null || topicsRegex == null) {
            return ResponseEntity.badRequest().body("Invalid request. 'name' and 'topicsRegex' are required.");
        }

        String connectorConfig = generateSinkConnectorConfig(connectorName, topicsRegex);

        // Kafka Connect에 생성된 설정 파일 전달
        boolean success = sendConnectorConfigToKafkaConnect(connectorConfig);

        if (success) {
            return ResponseEntity.ok("Connector created successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to create Connector");
        }
    }

    private boolean sendConnectorConfigToKafkaConnect(String connectorConfig) {
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

    private String generateSinkConnectorConfig(String connectorName, String topicsRegex) {
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
                        "tasks.max": "1",
                        "topics.regex": "%s"
                    }
                }
                """, connectorName,connectionUrl,connectionUser,connectionPassword,topicsRegex);
    }

    @DeleteMapping("/connectors/{connectorName}")
    public ResponseEntity<String> deleteConnector(@PathVariable String connectorName) {
        // Kafka Connect에 생성된 설정 파일 전달
        boolean success = deleteConnectorFromKafkaConnect(connectorName);

        if (success) {
            return ResponseEntity.ok("Connector deleted successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to delete Connector");
        }
    }
    private boolean deleteConnectorFromKafkaConnect(String connectorName) {
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

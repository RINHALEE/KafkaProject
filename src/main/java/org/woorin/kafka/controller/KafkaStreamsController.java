package org.woorin.kafka.controller;

import org.woorin.kafka.processing.UserActivityExitDetector;
import org.woorin.kafka.support.KafkaConnectorService;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.woorin.kafka.processing.DynamicBranchingStreams;
import org.woorin.kafka.processing.KafkaStreamsExit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class KafkaStreamsController {
    private KafkaStreams branchStreams;
    private KafkaStreams exitStreams;
    private KafkaStreams TestStreams;

    @Autowired
    private KafkaConnectorService kafkaConnectorService;

    @Autowired
    private DynamicBranchingStreams dynamicBranchingStreams; // StreamBranchingByPrice 인스턴스 주입

    @Autowired
    private KafkaStreamsExit kafkaStreamsExit; // StreamBranchingByPrice 인스턴스 주입

    @Autowired
    private UserActivityExitDetector kafkaExitStreams;
    // 생성된 커넥터 이름을 저장할 필드
    private List<String> createdConnectors = new ArrayList<>();

    // 분기 처리 시작
    @PostMapping("/branch/start")
    public ResponseEntity<String> startBranch(@RequestBody Map<String, String> criteria) {
        String criterion = criteria.getOrDefault("criterion", "");
        String value = criteria.getOrDefault("value", "");

        // 기존 스트림이 실행 중인 경우 중지
        if (branchStreams != null) {
            branchStreams.close();
            branchStreams = null;
        }

        // RequestBody로 요청한 분기 조건으로 스트림 생성 및 시작
        if (branchStreams ==null || !(branchStreams.state() == KafkaStreams.State.RUNNING || branchStreams.state() == KafkaStreams.State.REBALANCING)) {
            branchStreams = dynamicBranchingStreams.createBranchStreams(criterion, value); // Kafka Streams 생성 및 분기 처리 시작
            branchStreams.start();
        }

        String connectorName1 = "branch_"+criterion+(value.isEmpty() ? "_female" : "_above_"+value)+"_sink_connector";
        String connectorConfig1 = kafkaConnectorService.generateSinkConnectorConfig(connectorName1, (value.isEmpty() ? "female-topic" : "above-"+value+"-topic"));
        createdConnectors.add(connectorName1);

        String connectorName2 = "branch_"+criterion+(value.isEmpty() ? "_male" : "_below_"+value)+"_sink_connector";
        String connectorConfig2 = kafkaConnectorService.generateSinkConnectorConfig(connectorName2, (value.isEmpty() ? "male-topic" : "below-"+value+"-topic"));
        createdConnectors.add(connectorName2);

        // Kafka Connect에 생성된 설정 파일 전달
        boolean success = kafkaConnectorService.sendConnectorConfigToKafkaConnect(connectorConfig1);
        boolean success2 = kafkaConnectorService.sendConnectorConfigToKafkaConnect(connectorConfig2);

        if (success && success2) {
            return ResponseEntity.ok("Connector Name : " +connectorName1 + " and " + connectorName2 + " created successfully\n"+
                    "Branching started based on " + criterion + (value.isEmpty() ? "" : " with value: " + value));
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to create Connector");
        }
    }

    // 분기 처리 중지
    @GetMapping("/branch/stop")
    public String stopBranch() {
        if (branchStreams != null) {
            branchStreams.close();

            for (String connectorName : createdConnectors) {
                kafkaConnectorService.deleteConnectorFromKafkaConnect(connectorName);
            }
            createdConnectors.clear(); // 커넥터 목록 초기화
            return "branching stopped";
        }
        return "Streams are not running";
    }

    // 이탈 감지 시작
    @GetMapping("/exit/start")
    public String startExit() {
        if (exitStreams != null) {
            exitStreams.close();
            exitStreams = null;
        }

        if (exitStreams ==null || (exitStreams.state() == KafkaStreams.State.RUNNING || exitStreams.state() == KafkaStreams.State.REBALANCING)) {
            // 이탈감지
            exitStreams = kafkaExitStreams.createExitStreams(); // Kafka Streams 생성 및 이탈 감지 시작
            exitStreams.start();
        }

        String connectorName = "exit_sink_connector";
        String connectorConfig = kafkaConnectorService.generateSinkConnectorConfig(connectorName, "exit-event-topic");
        createdConnectors.add(connectorName);

        boolean success = kafkaConnectorService.sendConnectorConfigToKafkaConnect(connectorConfig);

        if (success) {
            return "Connector Name : " +connectorName + " created successfully\n"+
                    "Exiting started";
        } else {
            return "Failed to create Connector";
        }
    }

    // 이탈 감지 중지
    @GetMapping("/exit/stop")
    public String stopExit() {
        if (exitStreams != null) {
            exitStreams.close();

            for (String connectorName : createdConnectors) {
                kafkaConnectorService.deleteConnectorFromKafkaConnect(connectorName);
            }
            createdConnectors.clear(); // 커넥터 목록 초기화
            return "Exiting stopped";
        }
        return "Streams are not running";
    }

    public KafkaStreamsController() {
        // 종료 훅
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 프로그램 종료 시 커넥터 삭제
            for (String connectorName : createdConnectors) {
                kafkaConnectorService.deleteConnectorFromKafkaConnect(connectorName);
            }
        }));
    }
}


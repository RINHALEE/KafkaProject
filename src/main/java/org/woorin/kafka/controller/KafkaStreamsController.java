package org.woorin.kafka.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.woorin.kafka.processing.DynamicBranchingStreams;
import org.woorin.kafka.processing.KafkaStreamsExit;

import java.util.Map;

@RestController
public class KafkaStreamsController {
    private KafkaStreams Branchstreams;
    private KafkaStreams Exitstreams;

    @Autowired
    private DynamicBranchingStreams dynamicBranchingStreams; // StreamBranchingByPrice 인스턴스 주입

    @Autowired
    private KafkaStreamsExit kafkaStreamsExit; // StreamBranchingByPrice 인스턴스 주입

    // 분기 처리 시작
    @PostMapping("/branch/start")
    public ResponseEntity<String> startBranch(@RequestBody Map<String, String> criteria) {
        String criterion = criteria.getOrDefault("criterion", "");
        String value = criteria.getOrDefault("value", "");

        // 기존 스트림이 실행 중인 경우 중지
        if (Branchstreams != null) {
            Branchstreams.close();
            Branchstreams = null;
        }

        // RequestBody로 요청한 분기 조건으로 스트림 생성 및 시작
        if (Branchstreams==null || !(Branchstreams.state() == KafkaStreams.State.RUNNING || Branchstreams.state() == KafkaStreams.State.REBALANCING)) {
            Branchstreams = dynamicBranchingStreams.createBranchStreams(criterion, value); // Kafka Streams 생성 및 분기 처리 시작
            Branchstreams.start();
            return ResponseEntity.ok("Branching started based on " + criterion + (value.isEmpty() ? "" : " with value: " + value));
        }
        return ResponseEntity.ok("Streams are already running");
    }

    // 분기 처리 중지
    @GetMapping("/branch/stop")
    public String stopBranch() {
        if (Branchstreams != null) {
            Branchstreams.close();
            return "branching stopped";
        }
        return "Streams are not running";
    }

    // 이탈 감지 시작
    @GetMapping("/exit/start")
    public String startExit() {
        //
        if (Exitstreams != null) {
            Exitstreams.close();
            Exitstreams = null;
        }

        if (Exitstreams==null || (Exitstreams.state() == KafkaStreams.State.RUNNING || Exitstreams.state() == KafkaStreams.State.REBALANCING)) {
            // 이탈감지
            Exitstreams = kafkaStreamsExit.createExitStreams(); // Kafka Streams 생성 및 이탈 감지 시작
            Exitstreams.start();
            return "Exiting started";
        }
        return "Streams are already running";
    }

    // 이탈 감지 중지
    @GetMapping("/exit/stop")
    public String stopExit() {
        if (Exitstreams != null) {
            Exitstreams.close();
            return "Exiting stopped";
        }
        return "Streams are not running";
    }
}


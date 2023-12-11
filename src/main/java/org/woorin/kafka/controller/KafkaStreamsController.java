package org.woorin.kafka.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.woorin.kafka.processing.StreamBranchingByPrice;

@RestController
public class KafkaStreamsController {

    private KafkaStreams streams;

    @Autowired
    private StreamBranchingByPrice streamBranchingByPrice; // StreamBranchingByPrice 인스턴스 주입

    // 분기 처리 시작
    @GetMapping("/branch/start")
    public String startBranch() {
        if (streams == null || !(streams.state() == KafkaStreams.State.RUNNING || streams.state() == KafkaStreams.State.REBALANCING)) {
            streams = streamBranchingByPrice.createStreams(); // Kafka Streams 생성 및 분기 처리 시작
            streams.start();
            return "branching started";
        }
        return "Streams are already running";
    }

    // 분기 처리 중지
    @GetMapping("/branch/stop")
    public String stopBranch() {
        if (streams != null) {
            streams.close();
            return "branching stopped";
        }
        return "Streams are not running";
    }
}


package org.woorin.kafka.support;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class Producer {

    public static void main(String[] args) {
        // 카프카 서버 ip:port
        String kafkaServer = "192.168.56.101:9092";
        Random random = new Random();

        Properties configs = new Properties();
        configs.put("bootstrap.servers", kafkaServer); // kafka host 및 server 설정
        configs.put("acks", "all");                         // 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않습니다.
        configs.put("block.on.buffer.full", "true");        // 서버로 보낼 레코드를 버퍼링 할 때 사용할 수 있는 전체 메모리의 바이트수
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        // 날짜 포맷터 설정
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        // message 전달 (evt_time은 현재 시간으로 / uid, gender, price는 랜덤값으로 전달)
        while (true) {
            String currentTime = LocalDateTime.now().format(formatter);
            int uid = random.nextInt(900000000) + 100000000;
            String gender = random.nextBoolean() ? "M" : "F";
            int price = (random.nextInt(200) + 1) * 1000;
            String v = String.format("{\"t\":\"click event\"," +
                    "\"intag\":\"hcs.com/62\"," +
                    "\"evt_time\":\"%s\"," +
                    "\"Uid\":\"%s\"," +
                    "\"gender\":\"%s\"," +
                    "\"total_price\":\"%s\"}", currentTime, uid, gender, price);
            producer.send(new ProducerRecord<String, String>("web-log-topic", v));

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
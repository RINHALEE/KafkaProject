package org.woorin.kafka.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Iterator;
import java.util.Map;

public class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 데이터를 스키마 형식으로 변환하는 메소드
    public static JsonNode transformToSchemaFormat(JsonNode value) {
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

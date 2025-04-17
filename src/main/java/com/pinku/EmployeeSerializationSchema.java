package com.pinku;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinku.pojos.Employee;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EmployeeSerializationSchema implements KafkaSerializationSchema<Employee> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public EmployeeSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Employee employee, @Nullable Long timestamp) {
        try {
            // Serialize Employee object to JSON
            String value = objectMapper.writeValueAsString(employee);
            return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8)); // Explicit UTF-8 encoding
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Employee object to JSON", e);
        }
    }
}

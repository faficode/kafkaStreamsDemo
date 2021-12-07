package demo.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer

import static demo.kafka.Helpers.createObjectMapper

class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = createObjectMapper()

    @Override
    byte[] serialize(String topic, T data) {
        if (data == null)
            return null

        return objectMapper.writeValueAsBytes(data)
    }
}
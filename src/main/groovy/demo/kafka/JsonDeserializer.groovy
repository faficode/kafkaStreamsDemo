package demo.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer

import static demo.kafka.Helpers.createObjectMapper

class JsonDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = createObjectMapper()

    private Class<T> clazz;

    JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz
    }

    @Override
    T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        return objectMapper.readValue(bytes, clazz)
    }

}

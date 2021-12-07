package demo.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.DoubleSerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology

import java.time.Instant

class Helpers {
    static Admin createAdminClient(String servers) {
        Map<String, Object> props = [:]
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = servers

        Admin.create(props)
    }

    static KafkaProducer<String, String> createStringProducer(String servers) {
        def props = new Properties()
        props.put(ProducerConfig.ACKS_CONFIG, 'all')
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.name)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)

        return new KafkaProducer<String, String>(props)
    }

    static KafkaProducer<String, Integer> createIntProducer(String servers) {
        def props = new Properties()
        props.put(ProducerConfig.ACKS_CONFIG, 'all')
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.name)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)

        return new KafkaProducer<String, Integer>(props)
    }

    static KafkaConsumer<String, String> createConsumer(String servers) {
        def props = new Properties()
        props.put(ConsumerConfig.GROUP_ID_CONFIG, 'mail-client')
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.name)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)

        return new KafkaConsumer<String, String>(props)
    }

    static KafkaStreams createStream(String servers, String appId, Topology topology) {
        def props = new Properties()
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appId)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.STATE_DIR_CONFIG, '/tmp/kafkastreams')

        // set CACHE_MAX_BYTES_BUFFERING_CONFIG to 0 in case you want every event sent downstream
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000)

        return new KafkaStreams(topology, props)
    }

    static Date parseDate(String utcDate) {
        return Date.from(Instant.parse(utcDate))
    }

    static ObjectMapper createObjectMapper() {
        def mapper = new ObjectMapper()
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper
    }
}

package demo.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer

import static demo.kafka.Helpers.createIntProducer

class CostProducer {
    static void main(String[] args) {
        produceCosts('localhost:9092')
    }

    static void produceCosts(String servers) {
        def producer = createIntProducer(servers)

        def costs = [
                'C++': 220,
                'Java': 100,
                'Go': 60,
                'Python': 40,
                'R': 70,
                'Javascript': 65,
        ]

        costs.entrySet().each {
            producer.send(new ProducerRecord<String, Integer>('costs', it.key, it.value))
        }
        producer.flush()
        producer.close()
    }
}

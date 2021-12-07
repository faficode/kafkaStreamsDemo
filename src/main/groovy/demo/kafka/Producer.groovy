package demo.kafka

import org.apache.kafka.clients.producer.ProducerRecord

import static demo.kafka.Helpers.createStringProducer

class Producer {
    static void main(String[] args) {
        produceAlert('localhost:9092')
    }

    static void produceAlert(String servers) {
        def producer = createStringProducer(servers)

        producer.send(new ProducerRecord<String, String>('alerts', null, 'alert in system x'))
        producer.flush()
        producer.close()
    }
}

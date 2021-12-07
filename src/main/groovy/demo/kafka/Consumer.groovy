package demo.kafka

import java.time.Duration

import static demo.kafka.Helpers.createConsumer

class Consumer {
    static void main(String[] args) {
        consumeAlert('localhost:9092')

    }

    static void consumeAlert(String servers) {
        def consumer = createConsumer(servers)

        consumer.subscribe(['alerts'])

        while (true) {
            def records = consumer.poll(Duration.ofSeconds(1))

            records.each {
                println it.value()
            }
        }
    }
}

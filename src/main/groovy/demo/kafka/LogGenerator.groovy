package demo.kafka

import com.github.javafaker.Faker
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors

import static demo.kafka.Helpers.createStringProducer
import static java.time.Instant.now

class LogGenerator {
    private final static Logger LOG = LoggerFactory.getLogger(LogGenerator)
    private final Random random = new Random()
    private final Producer<String, String> producer

    LogGenerator(String servers) {
        producer = createStringProducer(servers)
    }

    static void main(String[] args) {
        def pool = Executors.newCachedThreadPool()
        1.times {
            pool.submit({
                new LogGenerator('localhost:9092').generate(100000)
            })
        }
    }

    void generate(int count) {
        LOG.info('start sending logs')

        generateInternal(count)

        producer.flush()
        producer.close()
        LOG.info('done sending logs')
    }

    void send(String topic, String logLine) {
        producer.send(new ProducerRecord<String, String>(topic, null, logLine))
    }

    void generateInternal(int count) {
        def levels = ['WARN', 'INFO', 'ERROR']
        def plNames = ['Java', 'Go', 'C++', 'Python', 'Javascript', 'R']

        def faker = new Faker()

        count.times {
            String plName = plNames.get(random.nextInt(plNames.size())).padRight(15)
            String taskId = UUID.randomUUID().toString().substring(0, 8)

            send('raw-logs', "${createBasePart(plName, taskId)}|start")

            sleep(faker.number().numberBetween(1000, 1500))

            random.nextInt(4).times {
                sleep(faker.number().numberBetween(10, 100))
                send('raw-logs', "${createBasePart(plName, taskId)}|${levels[random.nextInt(levels.size())]}: ${faker.file().fileName()}: ${faker.chuckNorris().fact()}")
            }

            send('raw-logs', "${createBasePart(plName, taskId)}|total lines of code: ${faker.number().numberBetween(500, 1000000)}")
            send('raw-logs', "${createBasePart(plName, taskId)}|end")

            sleep(faker.number().numberBetween(100, 500))
        }
    }

    private String createBasePart(String plName, String taskId) {
        "${now()}|$plName|$taskId"
    }
}

package demo.kafka

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*

import java.time.Duration

import static demo.kafka.Helpers.*

class LogStream {
    static Serde<Log> LOG_SERDE = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer<>(Log))
    static Serde<Range> RANGE_SERDE = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer<>(Range))

    static void main(String[] args) {
//        listTopics('localhost:9092')
//        createStream('localhost:9092', 'copyStream', copyStream()).start()
        createStream('localhost:9092', 'normalizeStream', normalizeStream()).start()
        createStream('localhost:9092', 'countTasksStream', countStream()).start()
        createStream('localhost:9092', 'durationTasksStream', computeDuration()).start()
        createStream('localhost:9092', 'costsStream', computeCostsStream()).start()
    }

    static Topology copyStream() {
        def builder = new StreamsBuilder()
        builder.stream('raw-logs')
                .to('copy-logs')

        return builder.build()
    }

    static Topology normalizeStream() {
        def builder = new StreamsBuilder()

        builder.stream('raw-logs')
                .mapValues({ String line ->
                    def parts = line.split('\\|')

                    return new Log(timestamp: parseDate(parts[0]), language: parts[1].trim(), taskId: parts[2], message: parts[3])
                } as ValueMapper)
                .to('normalized-logs', Produced.with(Serdes.String(), LOG_SERDE))

        return builder.build()
    }

    static Topology countStream() {
        def builder = new StreamsBuilder()

        builder.stream('normalized-logs', Consumed.with(Serdes.String(), LOG_SERDE))
                .filter({ key, log -> log.message == 'start' })
                .selectKey({ key, value -> value.language })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ofSeconds(1)))
                .count()
                .toStream()
                .map({ key, value -> new KeyValue<>(key.key(), value) })
                .to('count-tasks', Produced.with(Serdes.String(), Serdes.Long()))

        return builder.build()
    }

    static Topology computeDuration() {
        def builder = new StreamsBuilder()

        builder.stream('normalized-logs', Consumed.with(Serdes.String(), LOG_SERDE))
                .filter({ key, value -> value.message in ['start', 'end'] })
                .groupBy({ key, value -> value.taskId } as KeyValueMapper<String, Log, String>)
                .aggregate({ new Range() }, { key, log, range ->
                    if (!range.start || range.start > log.timestamp) {
                        range.start = log.timestamp
                    }

                    if (!range.end || range.end < log.timestamp) {
                        range.end = log.timestamp
                    }

                    range.language = log.language

                    if (range.start && range.end) {
                        range.duration = range.end.time - range.start.time
                    }

                    return range
                }, Materialized.with(Serdes.String(), RANGE_SERDE))
                .toStream()
                .filter({ key, value -> value.duration != 0 })
                .to('task-duration', Produced.with(Serdes.String(), RANGE_SERDE))

        return builder.build()
    }

    static Topology computeCostsStream() {
        def builder = new StreamsBuilder()

        def costs = builder.stream('costs', Consumed.with(Serdes.String(), Serdes.Integer()))
                .toTable()

        builder.stream('task-duration', Consumed.with(Serdes.String(), RANGE_SERDE))
                .selectKey({ key, value -> value.language })
                .join(costs, { left, right ->
                    left.duration * right
                }, Joined.with(Serdes.String(), RANGE_SERDE, Serdes.Integer()))
                .to('task-costs', Produced.with(Serdes.String(), Serdes.Long()))

        return builder.build()
    }

    static void listTopics(String servers) {

        def adminClient = createAdminClient(servers)
        adminClient.listTopics().names().get().each {
            println it
        }

//        adminClient.alterConsumerGroupOffsets('logApp1',
//                [new TopicPartition('raw-logs', 0): new OffsetAndMetadata(0)]
//        )
        adminClient.close()
    }

}

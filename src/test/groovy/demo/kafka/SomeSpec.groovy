package demo.kafka

import org.slf4j.LoggerFactory
import spock.lang.Specification

class SomeSpec extends Specification {
    def 'test'() {
        String hi = 'there'

        LoggerFactory.getLogger('test').info('hi ' + hi)

        expect:
        true
    }

    def 'date test'() {
        def end = new Date(10)
        def start = new Date(6)

        long duration = end.time - start.time

        println duration

        expect:
        true
    }
}

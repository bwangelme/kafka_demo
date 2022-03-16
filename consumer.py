import time

from confluent_kafka import Consumer
import msgpack
import log

COMMAND_NAME = 'consumer'
COMMAND_DESCRIPTION = 'kafka consumer'


logger = log.setup_logger(__name__)


def populate_argument_parser(subparser):
    pass


def main(args):
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        # 设置新的 group 从哪里开始读 msg, smallest, earliest, beginning 表示从头开始读
        # 'auto.offset.reset': 'smallest',
        # poll 调用的间隔超过 max.poll.interval.ms 时，就会报错
        # Application maximum poll interval (11000ms) exceeded by 2ms (adjust max.poll.interval.ms for long-running message processing): leaving group
        'max.poll.interval.ms': '11000',
        'session.timeout.ms': '10000',
        'group.id': 'group.lazy',
    })
    c.subscribe(['lazy'])
    logger.info("Start Consumer")
    while True:
        msg = c.poll(1.0)
        logger.info("Get msg %s", msg)
        time.sleep(20)
        if msg is None:
            continue
        if msg.error():
            logger.info("Consumer error: {}".format(msg.error()))
            continue

        v = msg.value()
        val = msgpack.loads(v)
        logger.info('Received message: {}/{}'.format(msg.offset(), val['name']))

    c.close()

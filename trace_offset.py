#!/usr/bin/env python3
# coding: utf-8

import io
import struct
from enum import IntEnum
from confluent_kafka import Consumer
import log

logger = log.setup_logger(__name__)


class MetadataKeyVersion(IntEnum):
    OFFSET = 1
    GROUP = 2


class OffsetKey:
    """
    OFFSET_COMMIT_KEY_SCHEMA
    """

    def __init__(self, k):
        assert isinstance(k, io.BytesIO)
        g_len, = struct.unpack(">h", k.read(2))
        group, = struct.unpack(">%ds" % g_len, k.read(g_len))
        self.group = group.decode()
        t_len, = struct.unpack(">h", k.read(2))
        topic, = struct.unpack(">%ds" % t_len, k.read(t_len))
        self.topic = topic.decode()
        self.partition, = struct.unpack(">l", k.read(4))

    def __repr__(self):
        return "<{},{},{}>".format(self.group, self.topic, self.partition)

    @property
    def metric(self):
        return "{}.{}.{}".format(self.group, self.topic, self.partition)


class OffsetValue_V0(object):
    '''
    OFFSET_COMMIT_VALUE_SCHEMA_V0
    '''

    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        self.offset, = struct.unpack(">q", v.read(8))
        m_len, = struct.unpack(">h", v.read(2))
        self.metadata, = struct.unpack(">%ds" % m_len, v.read(m_len))
        self.timestamp, = struct.unpack(">q", v.read(8))

    def __repr__(self):
        return "<{},{}>".format(self.offset, self.timestamp)


class OffsetValue_V1(object):
    '''
    OFFSET_COMMIT_VALUE_SCHEMA_V1
    '''

    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        self.offset, = struct.unpack(">q", v.read(8))
        m_len, = struct.unpack(">h", v.read(2))
        self.metadata, = struct.unpack(">%ds" % m_len, v.read(m_len))
        self.commit_timestamp, = struct.unpack(">q", v.read(8))
        self.expire_timestamp, = struct.unpack(">q", v.read(8))

    def __repr__(self):
        return "<{},{},{}>".format(self.offset, self.commit_timestamp,
                                   self.expire_timestamp)


class OffsetValue_V2(object):
    '''
    OFFSET_COMMIT_VALUE_SCHEMA_V2
    '''

    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        self.offset, = struct.unpack(">q", v.read(8))
        m_len, = struct.unpack(">h", v.read(2))
        self.metadata, = struct.unpack(">%ds" % m_len, v.read(m_len))
        self.commit_timestamp, = struct.unpack(">q", v.read(8))

    def __repr__(self):
        return "<{},{}>".format(self.offset, self.commit_timestamp)


class OffsetValue_V3(object):
    '''
    OFFSET_COMMIT_VALUE_SCHEMA_V3
    '''

    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        self.offset, = struct.unpack(">q", v.read(8))
        # TODO: NO_PARTITION_LEADER_EPOCH
        self.leader_epoch = struct.unpack(">l", v.read(4))
        m_len, = struct.unpack(">h", v.read(2))
        self.metadata, = struct.unpack(">%ds" % m_len, v.read(m_len))
        self.commit_timestamp, = struct.unpack(">q", v.read(8))

    def __repr__(self):
        return "<{},{},{}>".format(self.offset, self.leader_epoch,
                                   self.commit_timestamp)


class GroupKey(object):
    '''
    GROUP_METADATA_KEY_SCHEMA
    '''

    def __init__(self, k):
        assert isinstance(k, io.BytesIO)
        g_len, = struct.unpack(">h", k.read(2))
        group, = struct.unpack(">%ds" % g_len, k.read(g_len))
        self.group = group.decode()

    def __repr__(self):
        return "<{}>".format(self.group)


class GroupValue_V0(object):
    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        pt_len, = struct.unpack(">h", v.read(2))
        self.protocol_type, = struct.unpack(">%ds" % pt_len, v.read(pt_len))
        self.generation, = struct.unpack(">l", v.read(4))
        p_len, = struct.unpack(">h", v.read(2))
        self.protocol, = struct.unpack(">%ds" % p_len, v.read(p_len))
        l_len, = struct.unpack(">h", v.read(2))
        self.leader, = struct.unpack(">%ds" % l_len, v.read(l_len))
        # TODO MEMBER_METADATA_V0

    def __repr__(self):
        return "<{},{},{},{}>".format(self.protocol_type, self.generation,
                                      self.protocol, self.leader)


class GroupValue_V1(object):
    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        pt_len, = struct.unpack(">h", v.read(2))
        self.protocol_type, = struct.unpack(">%ds" % pt_len, v.read(pt_len))
        self.generation, = struct.unpack(">l", v.read(4))
        p_len, = struct.unpack(">h", v.read(2))
        # FIXME
        if p_len > 0:
            self.protocol, = struct.unpack(">%ds" % p_len, v.read(p_len))
        else:
            self.protocol = None
        l_len, = struct.unpack(">h", v.read(2))
        if l_len > 0:
            self.leader, = struct.unpack(">%ds" % l_len, v.read(l_len))
        else:
            self.leader = None
        # TODO MEMBER_METADATA_V1

    def __repr__(self):
        return "<{},{},{},{}>".format(self.protocol_type, self.generation,
                                      self.protocol, self.leader)


class GroupValue_V2(object):
    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        pt_len, = struct.unpack(">h", v.read(2))
        self.protocol_type, = struct.unpack(">%ds" % pt_len, v.read(pt_len))
        self.generation, = struct.unpack(">l", v.read(4))
        p_len, = struct.unpack(">h", v.read(2))
        # FIXME
        if p_len > 0:
            self.protocol, = struct.unpack(">%ds" % p_len, v.read(p_len))
        else:
            self.protocol = None
        l_len, = struct.unpack(">h", v.read(2))
        if l_len > 0:
            self.leader, = struct.unpack(">%ds" % l_len, v.read(l_len))
        else:
            self.leader = None
        self.current_state_timestamp = struct.unpack(">q", v.read(8))

    def __repr__(self):
        return "<{},{},{},{},{}>".format(self.protocol_type, self.generation,
                                         self.protocol, self.leader,
                                         self.current_state_timestamp)


class GroupValue_V3(object):
    def __init__(self, v):
        assert isinstance(v, io.BytesIO)
        pt_len, = struct.unpack(">h", v.read(2))
        self.protocol_type, = struct.unpack(">%ds" % pt_len, v.read(pt_len))
        self.generation, = struct.unpack(">l", v.read(4))
        p_len, = struct.unpack(">h", v.read(2))
        # FIXME
        if p_len > 0:
            self.protocol, = struct.unpack(">%ds" % p_len, v.read(p_len))
        else:
            self.protocol = None
        l_len, = struct.unpack(">h", v.read(2))
        if l_len > 0:
            self.leader, = struct.unpack(">%ds" % l_len, v.read(l_len))
        else:
            self.leader = None
        self.current_state_timestamp = struct.unpack(">q", v.read(8))

    def __repr__(self):
        return "<{},{},{},{},{}>".format(self.protocol_type, self.generation,
                                         self.protocol, self.leader,
                                         self.current_state_timestamp)


class GroupMetadata:
    def __init__(self, key, value):
        k = io.BytesIO(key)
        self.key_version, = struct.unpack(">h", k.read(2))
        if value is None:
            self.value_version = None
            v = None
        else:
            v = io.BytesIO(value)
            self.value_version, = struct.unpack(">h", v.read(2))

        if self.key_version == MetadataKeyVersion.OFFSET:
            self.key = OffsetKey(k)
            if self.value_version == 0:
                self.value = OffsetValue_V0(v)
            elif self.value_version == 1:
                self.value = OffsetValue_V1(v)
            elif self.value_version == 2:
                self.value = OffsetValue_V2(v)
            elif self.value_version == 3:
                self.value = OffsetValue_V3(v)
            elif self.value_version is None:
                self.value = None
            else:
                raise ValueError(
                    "Offset value schema version %s is not supported." % self.value_version)
        elif self.key_version == MetadataKeyVersion.GROUP:
            self.key = GroupKey(k)
            if self.value_version == 0:
                self.value = GroupValue_V0(v)
            elif self.value_version == 1:
                self.value = GroupValue_V1(v)
            elif self.value_version == 2:
                self.value = GroupValue_V2(v)
            elif self.value_version == 3:
                self.value = GroupValue_V3(v)
            elif self.value_version is None:
                self.value = None
            else:
                raise ValueError(
                    "Group value schema version %s is not supported." % self.value_version)
        else:
            raise ValueError(
                "Key schema version %s is not supported." % self.key_version)

    def __repr__(self):
        return "{},{}".format(self.key, self.value)


def track():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dba_kafka_offsets',
    })
    consumer.subscribe(topics=['__consumer_offsets', ])
    logger.info("Start tracker")

    while True:
        message = consumer.poll(1)
        logger.debug("Get message %s", message)
        if message is None:
            continue
        key = message.key()
        val = message.value()
        metadata = GroupMetadata(key, val)
        if val is None:
            logger.info("metadata %s", metadata)
            continue
        if not isinstance(metadata.key, OffsetKey):
            continue
        logger.info("key: %s, val: %s", metadata.key, metadata.value)
        print(metadata.key.metric, metadata.value.offset)


if __name__ == "__main__":
    track()

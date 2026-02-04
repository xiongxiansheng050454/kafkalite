package com.kafkalite.client.consumer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Topic-Partition 标识类
 */
@Getter
@RequiredArgsConstructor
public class TopicPartition {
    private final String topic;
    private final int partition;

    @Override
    public String toString() {
        return topic + "-" + partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartition that)) return false;
        return partition == that.partition && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return 31 * topic.hashCode() + partition;
    }
}
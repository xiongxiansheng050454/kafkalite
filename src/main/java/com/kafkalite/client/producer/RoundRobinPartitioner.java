package com.kafkalite.client.producer;

import com.kafkalite.model.Topic;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询策略（无Key时推荐）
 */
public class RoundRobinPartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(Topic topic, byte[] key, byte[] value, int numPartitions) {
        int next = counter.getAndIncrement() & 0x7fffffff;
        return next % numPartitions;
    }

    @Override
    public void close() {}
}
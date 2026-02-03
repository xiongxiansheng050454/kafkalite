package com.kafkalite.client.producer;

import com.kafkalite.model.Topic;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机策略（测试用途）
 */
public class RandomPartitioner implements Partitioner {
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    public int partition(Topic topic, byte[] key, byte[] value, int numPartitions) {
        return random.nextInt(numPartitions);
    }

    @Override
    public void close() {}
}

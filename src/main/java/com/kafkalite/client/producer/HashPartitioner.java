package com.kafkalite.client.producer;

import com.kafkalite.model.Topic;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Key Hash 策略（默认）
 */
public class HashPartitioner implements Partitioner {
    @Override
    public int partition(Topic topic, byte[] key, byte[] value, int numPartitions) {

        if (numPartitions <= 0) {
            throw new IllegalStateException("Invalid partition count: " + numPartitions);
        }

        if (key == null || key.length == 0) {
            // 无Key时轮询（更均匀）
            return ThreadLocalRandom.current().nextInt(numPartitions);
        }
        // 与Kafka一致：murmur2 hash & 0x7fffffff
        int hash = murmur2(key);
        return Math.abs(hash) % numPartitions;
    }

    // Kafka标准murmur2算法实现
    private int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = ((data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24));
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    @Override
    public void close() {}
}

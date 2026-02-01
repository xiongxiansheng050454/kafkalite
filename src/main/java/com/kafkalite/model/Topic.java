package com.kafkalite.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private final String name;
    private final int numPartitions;
    private final Map<Integer, Partition> partitions;

    public Topic(String name, int numPartitions) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.partitions = new ConcurrentHashMap<>();

        // 初始化分区
        for (int i = 0; i < numPartitions; i++) {
            partitions.put(i, new Partition(i, name));
        }
    }

    /**
     * 根据key选择分区（简化版：hash）
     */
    public Partition selectPartition(byte[] key) {
        if (key == null || key.length == 0) {
            // 轮询（这里简化，直接取第一个可用）
            return partitions.get(0);
        }
        int hash = 0;
        for (byte b : key) hash = 31 * hash + b;
        return partitions.get(Math.abs(hash) % numPartitions);
    }

    public Partition getPartition(int id) {
        return partitions.get(id);
    }

    public String getName() { return name; }
    public int getNumPartitions() { return numPartitions; }
}

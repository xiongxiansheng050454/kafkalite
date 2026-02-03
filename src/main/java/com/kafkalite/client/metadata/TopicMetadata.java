package com.kafkalite.client.metadata;

import lombok.Data;

import java.util.*;

/**
 * Topic 元数据，包含分区信息
 */
@Data
public class TopicMetadata {
    private final String topic;
    private final int partitionCount;
    private final List<PartitionInfo> partitions;
    private final long updateTimeMs;
    private final int epoch; // 版本号，用于检测变更

    public TopicMetadata(String topic, int partitionCount, int epoch) {
        this.topic = topic;
        this.partitionCount = partitionCount;
        this.epoch = epoch;
        this.updateTimeMs = System.currentTimeMillis();

        // 初始化分区信息（Day 3简化版，Day 3以后加入Leader信息）
        List<PartitionInfo> list = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            list.add(new PartitionInfo(topic, i, null)); // leaderNode暂时为null
        }
        this.partitions = Collections.unmodifiableList(list);
    }

    /**
     * @param leader Day 3以后实现多节点时使用
     */
    public record PartitionInfo(String topic, int partition, Node leader) {}

    public record Node(int id, String host, int port) {}
}
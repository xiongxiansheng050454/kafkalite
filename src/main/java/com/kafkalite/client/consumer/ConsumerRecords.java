package com.kafkalite.client.consumer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 消费记录集合 - 提供按分区遍历能力
 */
public class ConsumerRecords implements Iterable<ConsumerRecord> {
    private final Map<TopicPartition, List<ConsumerRecord>> records;
    private final List<ConsumerRecord> allRecords;

    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord>> records) {
        this.records = Collections.unmodifiableMap(records);
        this.allRecords = records.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public List<ConsumerRecord> records(TopicPartition partition) {
        return records.getOrDefault(partition, Collections.emptyList());
    }

    public Set<TopicPartition> partitions() {
        return records.keySet();
    }

    public boolean isEmpty() {
        return allRecords.isEmpty();
    }

    public int count() {
        return allRecords.size();
    }

    @Override
    public Iterator<ConsumerRecord> iterator() {
        return allRecords.iterator();
    }

    public static ConsumerRecords empty() {
        return new ConsumerRecords(Collections.emptyMap());
    }
}

package com.kafkalite.client.producer;

import com.kafkalite.model.Topic;

/**
 * 分区策略接口
 */
public interface Partitioner {
    /**
     * 计算分区号
     * @param topic 主题元数据
     * @param key 消息key
     * @param value 消息value
     * @param numPartitions 总分区数
     * @return 目标分区号
     */
    int partition(Topic topic, byte[] key, byte[] value, int numPartitions);

    void close();
}
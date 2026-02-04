package com.kafkalite.client.consumer;

import com.kafkalite.model.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 消费者记录 - 包含消息及其元数据
 */
@Getter
@RequiredArgsConstructor
public class ConsumerRecord {
    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] key;
    private final byte[] value;
    private final long timestamp;

    public static ConsumerRecord fromMessage(String topic, int partition, long offset, Message msg) {
        return new ConsumerRecord(
                topic,
                partition,
                offset,
                msg.getKey(),
                msg.getValue(),
                msg.getTimestamp() // 或者使用msg中的timestamp字段
        );
    }

    public String valueAsString() {
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }
}
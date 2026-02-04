package com.kafkalite.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 消息集合（批量），类似 Kafka RecordBatch
 * 格式: [4字节总长度][消息1][消息2]...
 */
public class MessageSet {
    private final List<Message> messages = new ArrayList<>();

    public void append(Message msg) {
        messages.add(msg);
    }

    public ByteBuffer toByteBuffer() {
        // 计算总大小
        int totalSize = 4; // 长度字段
        List<ByteBuffer> serialized = new ArrayList<>();
        for (Message m : messages) {
            ByteBuffer buf = m.serialize();
            serialized.add(buf);
            totalSize += buf.remaining();
        }

        ByteBuffer result = ByteBuffer.allocate(totalSize);
        result.putInt(totalSize - 4); // 写入总长度（不含自身）
        for (ByteBuffer buf : serialized) {
            result.put(buf);
        }
        result.flip();
        return result;
    }

    // 现有方法保持不变（用于ProduceRequest/FetchResponse的协议解码）
    public static List<Message> fromByteBuffer(ByteBuffer buffer) {
        return fromByteBuffer(buffer, 0); // 默认从0开始，offset不重要场景使用
    }

    // 新增：支持指定起始offset（用于MemoryQueue.read，需要正确offset）
    public static List<Message> fromByteBuffer(ByteBuffer buffer, long startOffset) {
        List<Message> result = new ArrayList<>();
        int totalLength = buffer.getInt();
        int end = buffer.position() + totalLength;
        long currentOffset = startOffset;

        while (buffer.hasRemaining() && buffer.position() < end) {
            // 简单起见，每次读取一个完整Message（实际Kafka有更复杂的变长编码）
            result.add(Message.deserialize(buffer, currentOffset++));
        }
        return result;
    }

    public int size() { return messages.size(); }
    public List<Message> getMessages() { return messages; }
}

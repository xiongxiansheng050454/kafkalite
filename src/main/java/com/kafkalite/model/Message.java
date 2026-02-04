package com.kafkalite.model;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class Message {
    // 消息格式: [4字节魔数][8字节offset][4字节key长度][key字节][4字节value长度][value字节][8字节timestamp]
    public static final int MAGIC = 0xCAFE;
    private static final AtomicLong OFFSET_GENERATOR = new AtomicLong(0);

    // Getters
    @Getter
    private final long offset;
    @Getter
    private final byte[] key;
    @Getter
    private final byte[] value;
    @Getter
    private final long timestamp;

    // 保留旧构造函数用于Producer创建消息（offset临时为0，由Partition分配）
    public Message(byte[] key, byte[] value) {
        this(key, value, -1); // -1 表示未分配
    }

    // 新增：用于Broker分配实际offset或Consumer反序列化
    public Message(byte[] key, byte[] value, long offset) {
        this.offset = offset;
        this.key = key != null ? key : new byte[0];
        this.value = value != null ? value : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    public ByteBuffer serialize() {
        int totalSize = 4 + 8 + 4 + key.length + 4 + value.length + 8;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(MAGIC);
        buffer.putLong(offset);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.putLong(timestamp);
        buffer.flip(); // 切换为读模式
        return buffer;
    }

    public static Message deserialize(ByteBuffer buffer, long offset) {
        buffer.getInt();
        // 跳过魔数校验（生产环境需验证）
        buffer.getLong();
        // 跳过offset，实际应从log中读取
        int keyLen = buffer.getInt();
        byte[] key = new byte[keyLen];
        buffer.get(key);
        int valueLen = buffer.getInt();
        byte[] value = new byte[valueLen];
        buffer.get(value);
        buffer.getLong();
        // 跳过timestamp
        return new Message(key, value, offset);
    }

    public String getValueAsString() { return new String(value, StandardCharsets.UTF_8); }
}

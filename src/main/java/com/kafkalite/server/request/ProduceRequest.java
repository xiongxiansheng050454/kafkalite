package com.kafkalite.server.request;

import com.kafkalite.model.MessageSet;
import com.kafkalite.server.protocol.RequestType;
import io.netty.buffer.ByteBuf;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

@EqualsAndHashCode(callSuper = true)
public class ProduceRequest extends AbstractRequest{

    private MessageSet messageSet;

    @Override
    public RequestType getRequestType() { return RequestType.PRODUCE; }

    @Override
    public void decode(ByteBuf buf) {
        // topic
        short topicLen = buf.readShort();
        byte[] topicBytes = new byte[topicLen];
        buf.readBytes(topicBytes);
        this.topic = new String(topicBytes);

        // partition
        this.partition = buf.readInt();

        // MessageSet（长度 + 字节）
        int msgSetLen = buf.readInt();
        byte[] msgSetBytes = new byte[msgSetLen];
        buf.readBytes(msgSetBytes);

        // 包装成 ByteBuffer 反序列化（复用 Day1 代码）
        ByteBuffer nioBuf = ByteBuffer.wrap(msgSetBytes);
        this.messageSet = new MessageSet();
        var messages = MessageSet.fromByteBuffer(nioBuf);
        messages.forEach(this.messageSet::append);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeShort(topic.length());
        buf.writeBytes(topic.getBytes());
        buf.writeInt(partition);

        ByteBuffer nioBuf = messageSet.toByteBuffer();
        byte[] bytes = new byte[nioBuf.remaining()];
        nioBuf.get(bytes);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    // Getter/Setter
    public MessageSet getMessageSet() { return messageSet; }
    public void setMessageSet(MessageSet messageSet) { this.messageSet = messageSet; }
}

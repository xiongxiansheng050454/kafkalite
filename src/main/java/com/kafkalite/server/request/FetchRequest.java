package com.kafkalite.server.request;

import com.kafkalite.server.protocol.RequestType;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class FetchRequest extends AbstractRequest{

    private long offset;
    private int maxBytes;

    @Override
    public RequestType getRequestType() { return RequestType.FETCH; }

    @Override
    public void decode(ByteBuf buf) {
        short topicLen = buf.readShort();
        byte[] topicBytes = new byte[topicLen];
        buf.readBytes(topicBytes);
        this.topic = new String(topicBytes);

        this.partition = buf.readInt();
        this.offset = buf.readLong();
        this.maxBytes = buf.readInt();
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeShort(topic.length());
        buf.writeBytes(topic.getBytes());
        buf.writeInt(partition);
        buf.writeLong(offset);
        buf.writeInt(maxBytes);
    }
}

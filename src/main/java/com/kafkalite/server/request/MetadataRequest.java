package com.kafkalite.server.request;

import com.kafkalite.server.protocol.RequestType;
import io.netty.buffer.ByteBuf;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class MetadataRequest extends AbstractRequest{

    @Override
    public RequestType getRequestType() { return RequestType.METADATA; }

    @Override
    public void decode(ByteBuf buf) {
        short topicLen = buf.readShort();
        if (topicLen > 0) {
            byte[] bytes = new byte[topicLen];
            buf.readBytes(bytes);
            this.topic = new String(bytes);
        }
    }
    @Override
    public void encode(ByteBuf buf) {
        if (topic == null) {
            buf.writeShort(0);
        } else {
            buf.writeShort(topic.length());
            buf.writeBytes(topic.getBytes());
        }
    }
}

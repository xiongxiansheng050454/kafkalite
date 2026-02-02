package com.kafkalite.server.response;

import com.kafkalite.server.protocol.ResponseType;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataResponse extends AbstractResponse{
    // topic -> partition count
    private Map<String, Integer> topicMetadata = new HashMap<>();

    @Override
    public ResponseType getResponseType() { return ResponseType.METADATA; }

    @Override
    public void decode(ByteBuf buf) {
        this.errorCode = buf.readShort();
        int size = buf.readInt();
        this.topicMetadata = new HashMap<>();
        for (int i = 0; i < size; i++) {
            short topicLen = buf.readShort();
            byte[] topicBytes = new byte[topicLen];
            buf.readBytes(topicBytes);
            String topic = new String(topicBytes);
            int partitions = buf.readInt();
            topicMetadata.put(topic, partitions);
        }
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeShort(errorCode);
        buf.writeInt(topicMetadata.size());
        topicMetadata.forEach((topic, partitions) -> {
            buf.writeShort(topic.length());
            buf.writeBytes(topic.getBytes());
            buf.writeInt(partitions);
        });
    }
}

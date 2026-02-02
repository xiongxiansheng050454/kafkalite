package com.kafkalite.server.response;

import com.kafkalite.model.Message;
import com.kafkalite.model.MessageSet;
import com.kafkalite.server.protocol.ResponseType;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class FetchResponse extends AbstractResponse{

    private MessageSet messageSet = new MessageSet();
    private long highWatermark; // 暂时返回 logEndOffset

    @Override
    public ResponseType getResponseType() { return ResponseType.FETCH; }

    @Override
    public void decode(ByteBuf buf) {
        this.errorCode = buf.readShort();
        this.highWatermark = buf.readLong();
        int msgSetLen = buf.readInt();
        byte[] msgSetBytes = new byte[msgSetLen];
        buf.readBytes(msgSetBytes);
        ByteBuffer nioBuf = ByteBuffer.wrap(msgSetBytes);
        this.messageSet = new MessageSet();
        MessageSet.fromByteBuffer(nioBuf).forEach(this.messageSet::append);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeShort(errorCode);
        buf.writeLong(highWatermark);

        // 序列化 MessageSet
        ByteBuffer nioBuf = messageSet.toByteBuffer();
        byte[] bytes = new byte[nioBuf.remaining()];
        nioBuf.get(bytes);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    public void setMessages(List<Message> messages) {
        messages.forEach(messageSet::append);
    }
}

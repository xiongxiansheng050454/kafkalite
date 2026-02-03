package com.kafkalite.server.protocol;

import com.kafkalite.server.request.AbstractRequest;
import com.kafkalite.server.request.FetchRequest;
import com.kafkalite.server.request.MetadataRequest;
import com.kafkalite.server.request.ProduceRequest;
import com.kafkalite.server.response.AbstractResponse;
import com.kafkalite.server.response.FetchResponse;
import com.kafkalite.server.response.MetadataResponse;
import com.kafkalite.server.response.ProduceResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ProtocolCodec {

    // ==================== 服务端：入站解码器（读取客户端请求）====================
    public static class RequestDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < 4) return;

            in.markReaderIndex();
            int length = in.readInt();

            // 现在长度包含：correlationId(8) + type(1) + payload
            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }

            // 1. 读取 correlationId（新增）
            long correlationId = in.readLong();

            // 2. 读取类型
            byte typeCode = in.readByte();
            RequestType type = RequestType.fromCode(typeCode);

            // 3. 读取 payload（长度减去 correlationId 和 type 占用的 9 字节）
            ByteBuf payload = in.readSlice(length - 9);
            AbstractRequest request;

            switch (type) {
                case PRODUCE -> request = new ProduceRequest();
                case FETCH -> request = new FetchRequest();
                case METADATA -> request = new MetadataRequest();
                default -> {
                    log.error("Unknown request type: {}", typeCode);
                    return;
                }
            }

            request.setCorrelationId(correlationId); // 设置到请求对象
            request.decode(payload);
            out.add(request);
        }
    }

    // ==================== 服务端：出站编码器（发送响应给客户端）====================
    public static class ResponseEncoder extends MessageToByteEncoder<AbstractResponse> {
        @Override
        protected void encode(ChannelHandlerContext ctx, AbstractResponse response, ByteBuf out) {
            int lengthIdx = out.writerIndex();
            out.writeInt(0); // 长度占位

            // 1. 写入 correlationId（必须回传客户端传来的 ID）
            out.writeLong(response.getCorrelationId());

            // 2. 写入类型
            out.writeByte(response.getResponseType().code());

            // 3. 写入 payload
            response.encode(out);

            // 回填长度（correlationId + type + payload 的总长度）
            int totalLen = out.writerIndex() - lengthIdx - 4;
            out.setInt(lengthIdx, totalLen);
        }
    }

    // ==================== 客户端：出站编码器（发送请求给服务端）====================
    public static class RequestEncoder extends MessageToByteEncoder<AbstractRequest> {
        @Override
        protected void encode(ChannelHandlerContext ctx, AbstractRequest request, ByteBuf out) {
            int lengthIdx = out.writerIndex();
            out.writeInt(0); // 长度占位

            // 1. 写入 correlationId
            out.writeLong(request.getCorrelationId());

            // 2. 写入类型
            out.writeByte(request.getRequestType().code());

            // 3. 写入 payload
            request.encode(out);

            // 回填长度
            int totalLen = out.writerIndex() - lengthIdx - 4;
            out.setInt(lengthIdx, totalLen);
        }
    }

    // ==================== 客户端：入站解码器（读取服务端响应）====================
    public static class ResponseDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < 4) return;

            in.markReaderIndex();
            int length = in.readInt();

            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }

            // 1. 读取 correlationId
            long correlationId = in.readLong();

            // 2. 读取类型
            byte typeCode = in.readByte();
            ResponseType type = ResponseType.fromCode(typeCode);

            // 3. 读取 payload（减去 9 字节）
            ByteBuf payload = in.readSlice(length - 9);

            AbstractResponse response;

            switch (type) {
                case PRODUCE -> response = new ProduceResponse();
                case FETCH -> response = new FetchResponse();
                case METADATA -> response = new MetadataResponse();
                default -> {
                    log.error("Unknown response type: {}", typeCode);
                    return;
                }
            }

            response.setCorrelationId(correlationId);
            response.decode(payload);
            out.add(response);
        }
    }
}
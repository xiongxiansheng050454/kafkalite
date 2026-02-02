package com.kafkalite.client;

import com.kafkalite.server.protocol.ProtocolCodec;
import com.kafkalite.server.request.AbstractRequest;
import com.kafkalite.server.response.AbstractResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NetworkClient {
    private final String host;
    private final int port;
    private Channel channel;
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    private final ResponseHandler responseHandler = new ResponseHandler();

    public NetworkClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ProtocolCodec.RequestEncoder());
                        p.addLast(new ProtocolCodec.ResponseDecoder());
                        p.addLast(responseHandler);
                    }
                });

        channel = bootstrap.connect(host, port).sync().channel();
        log.info("Connected to broker at {}:{}", host, port);
    }

    public CompletableFuture<AbstractResponse> send(AbstractRequest request) {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        responseHandler.registerPending(channel.id(), future);

        channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(f.cause());
            }
        });

        return future.orTimeout(5, TimeUnit.SECONDS);
    }

    public void close() {
        if (channel != null) {
            // 关闭前确保所有消息都已发送并释放
            channel.flush();
            channel.close();
        }
        group.shutdownGracefully();
    }

    // 简单的响应处理器，实际需要根据 correlationId 匹配，这里简化
    @Slf4j
    static class ResponseHandler extends SimpleChannelInboundHandler<AbstractResponse> {
        // 简化版：直接通过 channel id 映射，生产环境需用 correlationId
        private final Map<ChannelId, CompletableFuture<AbstractResponse>> pending = new ConcurrentHashMap<>();

        public void registerPending(ChannelId id, CompletableFuture<AbstractResponse> future) {
            pending.put(id, future);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, AbstractResponse response) {
            CompletableFuture<AbstractResponse> future = pending.remove(ctx.channel().id());
            if (future != null) {
                future.complete(response);
            }
        }
    }
}
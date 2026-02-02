package com.kafkalite.server;

import com.kafkalite.server.protocol.ProtocolCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaBrokerServer {

    static {
        // 开发阶段使用 PARANOID，生产使用 SIMPLE 或 DISABLED
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    private final int port;

    public KafkaBrokerServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()); // 接收连接
        EventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory()); // 处理IO

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 编解码器
                            pipeline.addLast(new ProtocolCodec.RequestDecoder());
                            pipeline.addLast(new ProtocolCodec.ResponseEncoder());
                            // 业务处理器
                            pipeline.addLast(new BrokerHandler());
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            log.info("KafkaLite Broker started on port {}", port);

            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new KafkaBrokerServer(9092).start(); // Kafka 默认端口
    }
}

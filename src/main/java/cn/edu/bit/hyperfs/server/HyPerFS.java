package cn.edu.bit.hyperfs.server;

import cn.edu.bit.hyperfs.handler.HttpServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HyPerFS {
    private static final Logger logger = LoggerFactory.getLogger(HyPerFS.class);
    private final int port;

    public HyPerFS(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        logger.info("Starting HyPerFS server on port {}", port);
        // 创建Reactor线程组
        var bossGroup = new NioEventLoopGroup(1); // 用于接收连接
        var workerGroup = new NioEventLoopGroup(); // 用于处理网络IO
        var businessGroup = new NioEventLoopGroup(); // 用于处理业务逻辑

        try {
            var serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            ChannelPipeline channelPipeline = socketChannel.pipeline();
                            channelPipeline.addLast(new HttpServerCodec()); // HTTP 编解码器
                            channelPipeline.addLast(new io.netty.handler.codec.http.HttpServerExpectContinueHandler()); // 处理
                                                                                                                        // Expect:
                                                                                                                        // 100-continue
                            channelPipeline.addLast(new ChunkedWriteHandler()); // 支持异步发送大的码流
                            channelPipeline.addLast(businessGroup, new HttpServerHandler()); // 业务逻辑处理
                        }
                    });

            logger.info("HyPerFS Server started on port {}", port);
            System.out.println("HyPerFS Server started on port " + port);

            ChannelFuture channelFuture = serverBootstrap.bind(port).sync(); // 绑定端口并同步等待
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            logger.error("Server start failed", e);
            throw e;
        } finally {
            logger.info("Stopping HyPerFS server...");
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            businessGroup.shutdownGracefully();
            logger.info("HyPerFS server stopped");
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 14514;
        new HyPerFS(port).start();
    }
}

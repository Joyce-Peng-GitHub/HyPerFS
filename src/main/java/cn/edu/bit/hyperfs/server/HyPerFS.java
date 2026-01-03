package cn.edu.bit.hyperfs.server;

import cn.edu.bit.hyperfs.db.DatabaseFactory;
import cn.edu.bit.hyperfs.handler.HttpServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

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
        // 使用 DefaultEventExecutorGroup 处理阻塞业务逻辑，线程数设置为 32
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(32);

        // 预初始化数据库，确保在 shutdown hook 可以关闭它
        DatabaseFactory.getInstance();

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

            ChannelFuture channelFuture = serverBootstrap.bind(port).sync(); // 绑定端口并同步等待
            Channel serverChannel = channelFuture.channel();

            // 注册 JVM shutdown hook，处理 Ctrl+C 信号
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Received shutdown signal, initiating graceful shutdown...");

                // 关闭服务器通道，停止接收新连接
                if (serverChannel.isOpen()) {
                    serverChannel.close().syncUninterruptibly();
                }

                // 优雅关闭 Netty 线程组
                logger.info("Shutting down Netty event loop groups...");
                bossGroup.shutdownGracefully().syncUninterruptibly();
                workerGroup.shutdownGracefully().syncUninterruptibly();
                businessGroup.shutdownGracefully().syncUninterruptibly();
                logger.info("Netty event loop groups shut down");

                // 关闭数据库连接池
                DatabaseFactory.getInstance().shutdown();

                logger.info("HyPerFS server gracefully stopped");
            }, "shutdown-hook"));

            logger.info("HyPerFS Server started on port {}", port);

            serverChannel.closeFuture().sync();
        } catch (Exception exception) {
            logger.error("Server start failed", exception);
            throw exception;
        } finally {
            // 如果不是通过 shutdown hook 关闭的，也需要清理资源
            if (!bossGroup.isShuttingDown()) {
                logger.info("Stopping HyPerFS server...");
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
                businessGroup.shutdownGracefully();
                DatabaseFactory.getInstance().shutdown();
                logger.info("HyPerFS server stopped");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final int defaultPort = 80;
        int port = defaultPort;
        if (args.length > 0) {
            try {
                int parsedPort = Integer.parseInt(args[0]);
                if (parsedPort >= 1 && parsedPort <= 65535) {
                    port = parsedPort;
                } else {
                    logger.warn("Port number {} is out of valid range (1-65535), using default port {}", parsedPort,
                            defaultPort);
                }
            } catch (NumberFormatException e) {
                logger.warn("Failed to parse '{}' as port number, using default port {}", args[0], defaultPort);
            }
        }
        new HyPerFS(port).start();
    }
}

package cn.edu.bit.hyperfs.server;

import cn.edu.bit.hyperfs.handler.HttpServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HyPerFSServer {
    private final int port;

    public HyPerFSServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        // 创建Reactor线程组
        var bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()); // 用于接收连接
        var workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory()); // 用于处理IO

        try {
            var serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            ChannelPipeline channelPipeline = socketChannel.pipeline();
                            channelPipeline.addLast(new HttpServerCodec()); // HTTP 编解码器
                            channelPipeline.addLast(new HttpObjectAggregator(6553600)); // 限制最大消息大小
                            channelPipeline.addLast(new ChunkedWriteHandler()); // 支持异步发送大的码流
                            channelPipeline.addLast(new HttpServerHandler()); // 业务逻辑处理
                        }
                    });

            System.out.println("HyPerFS Server started on port " + port);

            ChannelFuture channelFuture = serverBootstrap.bind(port).sync(); // 绑定端口并同步等待
            channelFuture.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 14514;
        new HyPerFSServer(port).start();
    }
}

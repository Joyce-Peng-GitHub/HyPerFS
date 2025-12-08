package cn.edu.bit.hyperfs.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class HyPerFSServer {
    private final int port;

    public HyPerFSServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        // 创建 Reactor 线程组
        var bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()); // 接收连接
        var workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory()); // 处理IO

        try {
            var serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpServerInitializer()); // 下一步编写这个初始化器

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

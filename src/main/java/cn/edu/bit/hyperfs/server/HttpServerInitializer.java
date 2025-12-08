package cn.edu.bit.hyperfs.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) {
        ChannelPipeline channelPipeline = socketChannel.pipeline();
        channelPipeline.addLast(new HttpServerCodec()); // HTTP 编解码器
        channelPipeline.addLast(new HttpObjectAggregator(6553600)); // 限制最大消息大小
        channelPipeline.addLast(new ChunkedWriteHandler()); // 支持异步发送大的码流
        channelPipeline.addLast(new HttpServerHandler()); // 业务逻辑处理
    }
}

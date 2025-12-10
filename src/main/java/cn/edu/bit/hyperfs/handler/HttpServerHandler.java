package cn.edu.bit.hyperfs.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    // Add logger
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) {
        // 处理异常请求
        if (!request.decoderResult().isSuccess()) {
            sendResponse(context, BAD_REQUEST, "Bad Request");
            return;
        }

        // 路由分发
        String uri = request.uri();
        HttpMethod method = request.method();

        System.out.println("Received Request: " + method + " " + uri);

        if ("/list".equals(uri) && method.equals(HttpMethod.GET)) {
            // TODO: 生成可下载文件清单
            sendResponse(context, OK, "File list logic placeholder");
        } else if (uri.startsWith("/download") && method.equals(HttpMethod.GET)) {
            // TODO: 处理文件下载
            sendResponse(context, OK, "Files download logic placeholder");
        } else if ("/upload".equals(uri) && method.equals(HttpMethod.POST)) {
            // TODO: 处理文件上传
            sendResponse(context, OK, "File upload logic placeholder");
        } else {
            sendResponse(context, NOT_FOUND, "Not Found");
        }
    }

    private void sendResponse(ChannelHandlerContext context, HttpResponseStatus status, String content) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status,
                io.netty.buffer.Unpooled.copiedBuffer(content, CharsetUtil.UTF_8)
        );
        response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
        context.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unhandled exception in channel pipeline", cause);
        ctx.close();
    }
}
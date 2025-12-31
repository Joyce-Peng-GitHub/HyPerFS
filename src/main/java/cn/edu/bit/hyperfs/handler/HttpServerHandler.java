package cn.edu.bit.hyperfs.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.bit.hyperfs.service.FileUploadSession;

import java.io.File;

public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);

    public static final String DEFAULT_DATA_DIRECTORY = "./data/";
    public static final String DEFAULT_TMP_DIRECTORY = "./tmp/";
    public static final String DEFAULT_FILENAME = "unknown";

    private File dataDirectory = new File(DEFAULT_DATA_DIRECTORY);
    private File tmpDirectory = new File(DEFAULT_TMP_DIRECTORY);
    private FileUploadSession uploadSession = null;

    public HttpServerHandler() {
        super();
        if (!dataDirectory.exists()) {
            dataDirectory.mkdirs();
        }
        if (!tmpDirectory.exists()) {
            tmpDirectory.mkdirs();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, HttpObject message) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    }
}
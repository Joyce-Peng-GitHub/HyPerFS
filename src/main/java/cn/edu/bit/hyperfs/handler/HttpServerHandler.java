package cn.edu.bit.hyperfs.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.bit.hyperfs.db.FileMetaDao;
import cn.edu.bit.hyperfs.entity.FileMetaNode;
import cn.edu.bit.hyperfs.service.FileUploadSession;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;

public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);

    public static final String DEFAULT_DATA_DIRECTORY = "./data/";
    public static final String DEFAULT_TMP_DIRECTORY = "./tmp/";
    public static final String DEFAULT_FILENAME = "unknown";

    private File dataDirectory = new File(DEFAULT_DATA_DIRECTORY);
    private File tmpDirectory = new File(DEFAULT_TMP_DIRECTORY);
    private FileUploadSession uploadSession = null;
    private FileMetaNode node = null;
    private FileMetaDao fileMetaDao = new FileMetaDao();

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
        if (message instanceof HttpRequest request) {
            if (request.decoderResult().isFailure()) {
                System.out.println("Bad Request: " + request.uri());
                sendResponse(context, BAD_REQUEST, "{\"error\":\"Bad Request\"}");
                return;
            }

            System.out.println(request.method() + " " + request.uri());
            if (request.method().equals(HttpMethod.OPTIONS)) {
                // 处理预检请求
                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
                response.headers().set("Access-Control-Allow-Origin", "*");
                response.headers().set("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
                response.headers().set("Access-Control-Allow-Headers",
                        "Content-Type, Accept, X-File-Name, X-Parent-Id");
                context.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                return;
            } else if (request.method().equals(HttpMethod.POST) && request.uri().startsWith("/upload")) {
                handleUploadRequest(context, request);
            } else {
                sendResponse(context, NOT_FOUND, "{\"error\":\"Not Found\"}");
            }
        }

        if (message instanceof HttpContent content && uploadSession != null) {
            handleUploadContent(context, content);
        }
    }

    private boolean parseRequestHeaders(ChannelHandlerContext context, HttpRequest request) {
        String parentIdString = QueryStringDecoder.decodeComponent(request.headers().get("X-Parent-Id"));
        if (parentIdString == null) { // 缺少父节点ID，拒绝请求
            sendResponse(context, BAD_REQUEST, "{\"error\":\"Missing X-Parent-Id header\"}");
            return false;
        }
        long parentId;
        try {
            parentId = Long.parseLong(parentIdString);
        } catch (NumberFormatException exception) { // 父节点ID格式错误，拒绝请求
            sendResponse(context, BAD_REQUEST, "{\"error\":\"Invalid X-Parent-Id header\"}");
            return false;
        }

        String filename = QueryStringDecoder.decodeComponent(request.headers().get("X-File-Name"));
        if (filename == null) {
            sendResponse(context, BAD_REQUEST, "{\"error\":\"Missing X-File-Name header\"}");
            return false;
        }

        node = new FileMetaNode();
        node.setParentId(parentId);
        node.setName(filename);
        return true;
    }

    private void handleUploadRequest(ChannelHandlerContext context, HttpRequest request) throws Exception {
        if (!parseRequestHeaders(context, request)) {
            resetState();
            return;
        }

        try {
            uploadSession = new FileUploadSession(tmpDirectory);
        } catch (Exception e) {
            logger.error("Failed to create upload session", e);
            sendResponse(context, INTERNAL_SERVER_ERROR, "{\"error\":\"Failed to create upload session\"}");
            resetState();
            return;
        }

        // 处理 Expect: 100-continue (HTTP 协议规范)
        if (HttpUtil.is100ContinueExpected(request)) {
            context.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }
    }

    private void handleUploadContent(ChannelHandlerContext context, HttpContent content) throws Exception {
        var chunk = content.content();
        if (chunk.isReadable()) {
            uploadSession.processChunk(chunk);
        }

        if (content instanceof LastHttpContent) {
            handleUploadCompletion(context);
        }
    }

    private void handleUploadCompletion(ChannelHandlerContext context) throws Exception {
        try {
            var result = uploadSession.finish(dataDirectory);
            node.setHashValue(result.getHashValue());
            node.setSize(result.getFileSize());
            node.setUploadTime(System.currentTimeMillis()); // 设置上传时间为当前UTC时间
            fileMetaDao.insertFileMetaNode(node);

            String responseJson = "{\"status\":\"success\"}";
            sendResponse(context, HttpResponseStatus.OK, responseJson);
            System.out.println("File uploaded successfully: " + node.getName());
        } catch (Exception exception) {
            logger.error("Upload failed", exception);
            sendResponse(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"error\":\"Upload failed\"}");
        } finally {
            resetState();
        }
    }

    private void resetState() {
        uploadSession = null;
        node = null;
    }

    private void sendResponse(ChannelHandlerContext context, HttpResponseStatus status, String content) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status,
                io.netty.buffer.Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
        response.headers().set("Access-Control-Allow-Origin", "*");
        context.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unhandled exception in channel pipeline", cause);
        resetState();
        ctx.close();
    }
}
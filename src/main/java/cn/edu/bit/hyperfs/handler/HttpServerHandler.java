package cn.edu.bit.hyperfs.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.bit.hyperfs.service.FileService;
import cn.edu.bit.hyperfs.service.FileUploadSession;
import cn.edu.bit.hyperfs.entity.FileMetaEntity;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);

    public static final String DEFAULT_DATA_DIRECTORY = "./data/";
    public static final String DEFAULT_TMP_DIRECTORY = "./tmp/";
    public static final String DEFAULT_FILENAME = "unknown";

    private FileService fileService = new FileService();
    private FileUploadSession uploadSession = null;

    // 用于解析 multipart
    private String currentFilename = null;
    private long currentParentId = 0;
    private boolean isUploading = false;

    public HttpServerHandler() {
        super();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpRequest request) {
            handleHttpRequest(ctx, request);
        }

        if (msg instanceof HttpContent content) {
            handleHttpContent(ctx, content);
        }
    }

    // 简单的路由和处理逻辑
    // 为了简化，这里假设上传是 POST
    // /upload?parentId=0&filename=xxx，Body是纯二进制流，而不是multipart/form-data
    // 如果必须支持multipart，Netty有HttpPostRequestDecoder，但会复杂很多。
    // 根据"协议解析：分别对上传、下载、列表指令做出反应，可以先不实现具体的功能"，我先实现简单的API约定。

    // API 设计:
    // GET /list?parentId=0 -> JSON列表
    // POST /upload?parentId=0&filename=xxx -> Body为文件内容
    // GET /download?id=1 -> 下载文件
    // POST /delete?id=1 -> 删除
    // POST /folder?parentId=0&name=xxx -> 创建文件夹

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String path = decoder.path();

        if (HttpMethod.GET.equals(request.method())) {
            if ("/list".equals(path)) {
                handleList(ctx, decoder);
            } else if ("/download".equals(path)) {
                handleDownload(ctx, decoder);
            } else if ("/".equals(path) || "/index.html".equals(path) || "/favicon.ico".equals(path)) {
                handleStatic(ctx, path);
            } else {
                sendError(ctx, HttpResponseStatus.NOT_FOUND);
            }
        } else if (HttpMethod.POST.equals(request.method())) {
            if ("/upload".equals(path)) {
                handleUploadStart(ctx, decoder);
            } else if ("/delete".equals(path)) {
                handleDelete(ctx, decoder);
            } else if ("/folder".equals(path)) {
                handleCreateFolder(ctx, decoder);
            } else {
                sendError(ctx, HttpResponseStatus.NOT_FOUND);
            }
        } else {
            sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void handleHttpContent(ChannelHandlerContext ctx, HttpContent content) throws Exception {
        if (isUploading && uploadSession != null) {
            uploadSession.processChunk(content.content());

            if (content instanceof LastHttpContent) {
                fileService.completeUpload(uploadSession, currentParentId, currentFilename);
                uploadSession = null;
                isUploading = false;
                sendResponse(ctx, HttpResponseStatus.OK, "Upload successful");
            }
        }
    }

    private void handleList(ChannelHandlerContext ctx, QueryStringDecoder decoder) throws Exception {
        long parentId = getLongParam(decoder, "parentId", 0);
        List<FileMetaEntity> list = fileService.list(parentId);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(list);
        sendResponse(ctx, HttpResponseStatus.OK, json, "application/json");
    }

    private void handleDownload(ChannelHandlerContext ctx, QueryStringDecoder decoder) throws Exception {
        long id = getLongParam(decoder, "id", -1);
        if (id == -1) {
            sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Missing id parameter");
            return;
        }

        try {
            // 解析Range头，支持断点续传
            // 简单处理，暂不支持复杂的Range
            DefaultFileRegion region = fileService.startDownload(id, 0, Long.MAX_VALUE);

            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, region.count());
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
            // 这里应该设置Content-Disposition，但需要查询文件名，DatabaseService.getFileMeta已经查了一次，这里为了性能或者结构可以优化。
            // 简单起见，先不设置文件名，或者让startDownload返回更多信息。
            // 为了可以设置文件名，我们可以在Handler层先查一下Meta。

            ctx.write(response);
            ctx.writeAndFlush(region, ctx.newProgressivePromise());
            // 最后写一个LastHttpContent
            ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            // 如果不是keep-alive，关闭连接
            // if (!HttpUtil.isKeepAlive(request)) {
            // future.addListener(ChannelFutureListener.CLOSE);
            // }

        } catch (Exception e) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND, e.getMessage());
        }
    }

    private void handleUploadStart(ChannelHandlerContext ctx, QueryStringDecoder decoder) throws Exception {
        long parentId = getLongParam(decoder, "parentId", 0);
        String filename = getStringParam(decoder, "filename", DEFAULT_FILENAME);

        this.currentParentId = parentId;
        this.currentFilename = filename;
        this.uploadSession = fileService.startUpload();
        this.isUploading = true;
        // 不发送响应，等待数据传输完成
    }

    private void handleDelete(ChannelHandlerContext ctx, QueryStringDecoder decoder) {
        long id = getLongParam(decoder, "id", -1);
        if (id == -1) {
            sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Missing id parameter");
            return;
        }
        try {
            fileService.delete(id);
            sendResponse(ctx, HttpResponseStatus.OK, "Deletion successful");
        } catch (Exception e) {
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void handleCreateFolder(ChannelHandlerContext ctx, QueryStringDecoder decoder) {
        long parentId = getLongParam(decoder, "parentId", 0);
        String name = getStringParam(decoder, "name", "New Folder");
        try {
            long id = fileService.createFolder(parentId, name);
            sendResponse(ctx, HttpResponseStatus.OK, String.valueOf(id));
        } catch (Exception e) {
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void handleStatic(ChannelHandlerContext ctx, String path) throws Exception {
        if ("/favicon.ico".equals(path)) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
        }
        // Serve frontend.html
        try (var is = getClass().getResourceAsStream("/frontend.html")) {
            if (is == null) {
                sendError(ctx, HttpResponseStatus.NOT_FOUND, "frontend.html not found");
                return;
            }
            byte[] content = is.readAllBytes();
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(content));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response);
        }
    }

    private long getLongParam(QueryStringDecoder decoder, String name, long defaultValue) {
        if (decoder.parameters().containsKey(name)) {
            return Long.parseLong(decoder.parameters().get(name).get(0));
        }
        return defaultValue;
    }

    private String getStringParam(QueryStringDecoder decoder, String name, String defaultValue) {
        if (decoder.parameters().containsKey(name)) {
            return decoder.parameters().get(name).get(0);
        }
        return defaultValue;
    }

    private void sendResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        sendResponse(ctx, status, message, "text/plain; charset=UTF-8");
    }

    private void sendResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String message,
            String contentType) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(message, StandardCharsets.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        ctx.writeAndFlush(response);
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        sendError(ctx, status, status.reasonPhrase());
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        sendResponse(ctx, status, "Error: " + message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception caught while processing request", cause);
        if (ctx.channel().isActive()) {
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage());
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (uploadSession != null) {
            uploadSession.abort();
        }
        super.channelInactive(ctx);
    }
}
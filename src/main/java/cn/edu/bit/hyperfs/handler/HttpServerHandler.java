package cn.edu.bit.hyperfs.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.bit.hyperfs.service.FileDownloadResource;
import cn.edu.bit.hyperfs.service.FileService;
import cn.edu.bit.hyperfs.service.FileUploadSession;
import cn.edu.bit.hyperfs.entity.FileMetaEntity;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
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

    // For move operation (JSON body)
    private boolean isMoving = false;
    private StringBuilder moveJsonBuffer = new StringBuilder();

    // For rename operation
    private boolean isRenaming = false;
    private StringBuilder renameJsonBuffer = new StringBuilder();

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

    // API 设计:
    // GET /list?parentId=0 -> JSON列表
    // POST /upload?parentId=0&filename=xxx -> Body为文件内容
    // GET /download?id=1 -> 下载文件
    // POST /delete?id=1 -> 删除
    // POST /folder?parentId=0&name=xxx -> 创建文件夹

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest request) throws Exception {
        logger.info("Received request: {} {}", request.method(), request.uri());
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String path = decoder.path();
        logger.info("Parsed path: '{}'", path);
        logger.info("Raw request: {}", request);

        if (HttpMethod.GET.equals(request.method())) {
            if ("/list".equals(path)) {
                handleList(ctx, decoder);
            } else if ("/download".equals(path)) {
                handleDownload(ctx, decoder, request);
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
            } else if ("/move".equals(path)) {
                isMoving = true;
                moveJsonBuffer.setLength(0);
            } else if ("/rename".equals(path)) {
                isRenaming = true;
                renameJsonBuffer.setLength(0);
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
        } else if (isMoving) {
            moveJsonBuffer.append(content.content().toString(StandardCharsets.UTF_8));
            if (content instanceof LastHttpContent) {
                handleMoveJson(ctx);
                isMoving = false;
                moveJsonBuffer.setLength(0);
            }
        } else if (isRenaming) {
            renameJsonBuffer.append(content.content().toString(StandardCharsets.UTF_8));
            if (content instanceof LastHttpContent) {
                handleRenameJson(ctx);
                isRenaming = false;
                renameJsonBuffer.setLength(0);
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

    private void handleDownload(ChannelHandlerContext ctx, QueryStringDecoder decoder, HttpRequest request)
            throws Exception {
        long id = getLongParam(decoder, "id", -1);
        if (id == -1) {
            sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Missing id parameter");
            return;
        }

        try {
            // 解析Range头，支持断点续传
            // 简单处理，暂不支持复杂的Range
            FileDownloadResource downloadResource = fileService.startDownload(id, 0, Long.MAX_VALUE);
            DefaultFileRegion region = downloadResource.region();
            String filename = downloadResource.filename();
            // URL encode filename to support special characters
            String encodedFilename = java.net.URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");

            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, region.count());
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
            response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION,
                    "attachment; filename*=UTF-8''" + encodedFilename);

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            ctx.write(response);
            ctx.writeAndFlush(region, ctx.newProgressivePromise());
            // 最后写一个LastHttpContent
            ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            // 如果不是keep-alive，关闭连接
            if (!HttpUtil.isKeepAlive(request)) {
                future.addListener(ChannelFutureListener.CLOSE);
            }

        } catch (Exception e) {
            logger.error("Download failed", e);
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
            logger.error("Delete failed", e);
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
            logger.error("Create folder failed", e);
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void handleMoveJson(ChannelHandlerContext ctx) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(moveJsonBuffer.toString());
            // Safe parsing with defaults/checks
            if (!node.has("id") || !node.has("targetParentId")) {
                sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Missing id or targetParentId");
                return;
            }

            long id = node.get("id").asLong();
            long targetParentId = node.get("targetParentId").asLong();
            String strategy = node.has("strategy") ? node.get("strategy").asText() : "FAIL";

            logger.info("Handling move request (JSON): id={}, target={}, strategy={}", id, targetParentId, strategy);
            fileService.moveNode(id, targetParentId, strategy);
            sendResponse(ctx, HttpResponseStatus.OK, "Move successful");
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException e) {
            sendError(ctx, HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (Exception e) {
            logger.error("Move failed", e);
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void handleRenameJson(ChannelHandlerContext ctx) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(renameJsonBuffer.toString());

            if (!node.has("id") || !node.has("name")) {
                sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Missing id or name");
                return;
            }

            long id = node.get("id").asLong();
            String name = node.get("name").asText();

            logger.info("Handling rename request: id={}, name={}", id, name);
            fileService.renameNode(id, name);
            sendResponse(ctx, HttpResponseStatus.OK, "Rename successful");
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException e) {
            sendError(ctx, HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (Exception e) {
            logger.error("Rename failed", e);
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
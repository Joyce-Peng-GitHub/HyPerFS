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
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String DEFAULT_DATA_DIRECTORY = "./data/";
    public static final String DEFAULT_TMP_DIRECTORY = "./tmp/";
    public static final String DEFAULT_FILENAME = "unknown";

    private final FileService fileService = new FileService();
    private FileUploadSession uploadSession = null;

    // 用于解析 multipart
    private String currentFilename = null;
    private long currentParentId = 0;
    private boolean isUploading = false;

    // 用于移动操作 (JSON body)
    private boolean isMoving = false;
    private StringBuilder moveJsonBuffer = new StringBuilder();

    // 用于重命名操作
    private boolean isRenaming = false;
    private StringBuilder renameJsonBuffer = new StringBuilder();

    // 用于复制操作
    private boolean isCopying = false;
    private StringBuilder copyJsonBuffer = new StringBuilder();

    public HttpServerHandler() {
        super();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, HttpObject message) throws Exception {
        if (message instanceof HttpRequest request) {
            handleHttpRequest(context, request);
        }

        if (message instanceof HttpContent content) {
            handleHttpContent(context, content);
        }
    }

    // API 设计:
    // GET /list?parentId=0 -> JSON列表
    // POST /upload?parentId=0&filename=xxx -> Body为文件内容
    // GET /download?id=1 -> 下载文件
    // POST /delete?id=1 -> 删除
    // POST /folder?parentId=0&name=xxx -> 创建文件夹

    private void handleHttpRequest(ChannelHandlerContext context, HttpRequest request) throws Exception {
        logger.info("Received request: {} {}", request.method(), request.uri());
        var queryStringDecoder = new QueryStringDecoder(request.uri());
        var path = queryStringDecoder.path();
        logger.info("Parsed path: '{}'", path);
        logger.info("Raw request: {}", request);

        if (HttpMethod.GET.equals(request.method())) {
            if ("/list".equals(path)) {
                handleList(context, queryStringDecoder);
            } else if ("/download".equals(path)) {
                handleDownload(context, queryStringDecoder, request);
            } else if ("/".equals(path) || "/index.html".equals(path) || "/favicon.ico".equals(path)) {
                handleStatic(context, path);
            } else {
                sendError(context, HttpResponseStatus.NOT_FOUND);
            }
        } else if (HttpMethod.POST.equals(request.method())) {
            if ("/upload".equals(path)) {
                handleUploadStart(context, queryStringDecoder);
            } else if ("/delete".equals(path)) {
                handleDelete(context, queryStringDecoder);
            } else if ("/folder".equals(path)) {
                handleCreateFolder(context, queryStringDecoder);
            } else if ("/move".equals(path)) {
                isMoving = true;
                moveJsonBuffer.setLength(0);
            } else if ("/rename".equals(path)) {
                isRenaming = true;
                renameJsonBuffer.setLength(0);
            } else if ("/copy".equals(path)) {
                isCopying = true;
                copyJsonBuffer.setLength(0);
            } else {
                sendError(context, HttpResponseStatus.NOT_FOUND);
            }
        } else {
            sendError(context, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void handleHttpContent(ChannelHandlerContext context, HttpContent content) throws Exception {
        if (isUploading && uploadSession != null) {
            uploadSession.processChunk(content.content());

            if (content instanceof LastHttpContent) {
                fileService.completeUpload(uploadSession, currentParentId, currentFilename);
                uploadSession = null;
                isUploading = false;
                sendResponse(context, HttpResponseStatus.OK, "Upload successful");
            }
        } else if (isMoving) {
            moveJsonBuffer.append(content.content().toString(StandardCharsets.UTF_8));
            if (content instanceof LastHttpContent) {
                handleMoveJson(context);
                isMoving = false;
                moveJsonBuffer.setLength(0);
            }
        } else if (isRenaming) {
            renameJsonBuffer.append(content.content().toString(StandardCharsets.UTF_8));
            if (content instanceof LastHttpContent) {
                handleRenameJson(context);
                isRenaming = false;
                renameJsonBuffer.setLength(0);
            }
        } else if (isCopying) {
            copyJsonBuffer.append(content.content().toString(StandardCharsets.UTF_8));
            if (content instanceof LastHttpContent) {
                handleCopyJson(context);
                isCopying = false;
                copyJsonBuffer.setLength(0);
            }
        }
    }

    private void handleList(ChannelHandlerContext context, QueryStringDecoder queryStringDecoder) throws Exception {
        var parentId = getLongParam(queryStringDecoder, "parentId", 0);
        List<FileMetaEntity> list = fileService.list(parentId);
        var json = objectMapper.writeValueAsString(list);
        sendResponse(context, HttpResponseStatus.OK, json, "application/json");
    }

    private void handleDownload(ChannelHandlerContext context, QueryStringDecoder queryStringDecoder,
            HttpRequest request)
            throws Exception {
        var id = getLongParam(queryStringDecoder, "id", -1);
        if (id == -1) {
            sendError(context, HttpResponseStatus.BAD_REQUEST, "Missing id parameter");
            return;
        }

        try {
            // 解析Range头，支持断点续传
            // 简单处理，暂不支持复杂的Range
            var downloadResource = fileService.startDownload(id, 0, Long.MAX_VALUE);
            var region = downloadResource.region();
            var filename = downloadResource.filename();
            // 对文件名进行URL编码以支持特殊字符
            var encodedFilename = java.net.URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");

            var response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, region.count());
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
            response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION,
                    "attachment; filename*=UTF-8''" + encodedFilename);

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            context.write(response);
            context.writeAndFlush(region, context.newProgressivePromise());
            // 最后写一个LastHttpContent
            var future = context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            // 如果不是keep-alive，关闭连接
            if (!HttpUtil.isKeepAlive(request)) {
                future.addListener(ChannelFutureListener.CLOSE);
            }

        } catch (Exception exception) {
            logger.error("Download failed", exception);
            sendError(context, HttpResponseStatus.NOT_FOUND, exception.getMessage());
        }
    }

    private void handleUploadStart(ChannelHandlerContext context, QueryStringDecoder queryStringDecoder)
            throws Exception {
        var parentId = getLongParam(queryStringDecoder, "parentId", 0);
        var filename = getStringParam(queryStringDecoder, "filename", DEFAULT_FILENAME);

        this.currentParentId = parentId;
        this.currentFilename = filename;
        this.uploadSession = fileService.startUpload();
        this.isUploading = true;
        // 不发送响应，等待数据传输完成
    }

    private void handleDelete(ChannelHandlerContext context, QueryStringDecoder queryStringDecoder) {
        var id = getLongParam(queryStringDecoder, "id", -1);
        if (id == -1) {
            sendError(context, HttpResponseStatus.BAD_REQUEST, "Missing id parameter");
            return;
        }
        try {
            fileService.delete(id);
            sendResponse(context, HttpResponseStatus.OK, "Deletion successful");
        } catch (Exception exception) {
            logger.error("Delete failed", exception);
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }

    private void handleCreateFolder(ChannelHandlerContext context, QueryStringDecoder queryStringDecoder) {
        var parentId = getLongParam(queryStringDecoder, "parentId", 0);
        var name = getStringParam(queryStringDecoder, "name", "New Folder");
        try {
            var id = fileService.createFolder(parentId, name);
            sendResponse(context, HttpResponseStatus.OK, String.valueOf(id));
        } catch (Exception exception) {
            logger.error("Create folder failed", exception);
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }

    private void handleMoveJson(ChannelHandlerContext context) {
        try {
            var jsonNode = objectMapper.readTree(moveJsonBuffer.toString());
            // 安全解析，包含默认值和检查
            if (!jsonNode.has("id") || !jsonNode.has("targetParentId")) {
                sendError(context, HttpResponseStatus.BAD_REQUEST, "Missing id or targetParentId");
                return;
            }

            var id = jsonNode.get("id").asLong();
            var targetParentId = jsonNode.get("targetParentId").asLong();
            var strategy = jsonNode.has("strategy") ? jsonNode.get("strategy").asText() : "FAIL";

            logger.info("Handling move request (JSON): id={}, target={}, strategy={}", id, targetParentId, strategy);
            fileService.moveNode(id, targetParentId, strategy);
            sendResponse(context, HttpResponseStatus.OK, "Move successful");
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException exception) {
            sendError(context, HttpResponseStatus.CONFLICT, exception.getMessage());
        } catch (Exception exception) {
            logger.error("Move failed", exception);
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }

    private void handleRenameJson(ChannelHandlerContext context) {
        try {
            var jsonNode = objectMapper.readTree(renameJsonBuffer.toString());

            if (!jsonNode.has("id") || !jsonNode.has("name")) {
                sendError(context, HttpResponseStatus.BAD_REQUEST, "Missing id or name");
                return;
            }

            var id = jsonNode.get("id").asLong();
            var name = jsonNode.get("name").asText();

            logger.info("Handling rename request: id={}, name={}", id, name);
            fileService.renameNode(id, name);
            sendResponse(context, HttpResponseStatus.OK, "Rename successful");
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException exception) {
            sendError(context, HttpResponseStatus.CONFLICT, exception.getMessage());
        } catch (Exception exception) {
            logger.error("Rename failed", exception);
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }

    private void handleCopyJson(ChannelHandlerContext context) {
        try {
            var jsonNode = objectMapper.readTree(copyJsonBuffer.toString());

            if (!jsonNode.has("id") || !jsonNode.has("targetParentId")) {
                sendError(context, HttpResponseStatus.BAD_REQUEST, "Missing id or targetParentId");
                return;
            }

            var id = jsonNode.get("id").asLong();
            var targetParentId = jsonNode.get("targetParentId").asLong();
            var strategy = jsonNode.has("strategy") ? jsonNode.get("strategy").asText() : "FAIL";

            logger.info("Handling copy request: id={}, target={}, strategy={}", id, targetParentId, strategy);
            fileService.copyNode(id, targetParentId, strategy);
            sendResponse(context, HttpResponseStatus.OK, "Copy successful");
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException exception) {
            sendError(context, HttpResponseStatus.CONFLICT, exception.getMessage());
        } catch (Exception exception) {
            logger.error("Copy failed", exception);
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }

    private void handleStatic(ChannelHandlerContext context, String path) throws Exception {
        if ("/favicon.ico".equals(path)) {
            sendError(context, HttpResponseStatus.NOT_FOUND);
            return;
        }
        // 返回 frontend.html
        try (var inputStream = getClass().getResourceAsStream("/frontend.html")) {
            if (inputStream == null) {
                sendError(context, HttpResponseStatus.NOT_FOUND, "frontend.html not found");
                return;
            }
            byte[] content = inputStream.readAllBytes();
            var response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(content));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            context.writeAndFlush(response);
        }
    }

    private long getLongParam(QueryStringDecoder queryStringDecoder, String name, long defaultValue) {
        if (queryStringDecoder.parameters().containsKey(name)) {
            return Long.parseLong(queryStringDecoder.parameters().get(name).get(0));
        }
        return defaultValue;
    }

    private String getStringParam(QueryStringDecoder queryStringDecoder, String name, String defaultValue) {
        if (queryStringDecoder.parameters().containsKey(name)) {
            return queryStringDecoder.parameters().get(name).get(0);
        }
        return defaultValue;
    }

    private void sendResponse(ChannelHandlerContext context, HttpResponseStatus status, String message) {
        sendResponse(context, status, message, "text/plain; charset=UTF-8");
    }

    private void sendResponse(ChannelHandlerContext context, HttpResponseStatus status, String message,
            String contentType) {
        var response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(message, StandardCharsets.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        context.writeAndFlush(response);
    }

    private void sendError(ChannelHandlerContext context, HttpResponseStatus status) {
        sendError(context, status, status.reasonPhrase());
    }

    private void sendError(ChannelHandlerContext context, HttpResponseStatus status, String message) {
        sendResponse(context, status, "Error: " + message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        logger.error("Exception caught while processing request", cause);
        if (context.channel().isActive()) {
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage());
        }
        context.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        if (uploadSession != null) {
            uploadSession.abort();
        }
        super.channelInactive(context);
    }
}
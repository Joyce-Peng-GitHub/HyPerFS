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

    // WebDAV XML Mapper
    private static final com.fasterxml.jackson.dataformat.xml.XmlMapper xmlMapper = new com.fasterxml.jackson.dataformat.xml.XmlMapper();

    public HttpServerHandler() {
        super();
        xmlMapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        xmlMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS);
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
    // POST /move, /rename, /copy -> JSON Body 操作
    // WebDAV: /webdav/* -> 支持 OPTIONS, PROPFIND, MKCOL, PUT, GET 等

    private void handleHttpRequest(ChannelHandlerContext context, HttpRequest request) throws Exception {
        logger.info("Received request: {} {}", request.method(), request.uri());
        var queryStringDecoder = new QueryStringDecoder(request.uri());
        var path = queryStringDecoder.path();
        logger.info("Parsed path: '{}'", path);

        // WebDAV 路由
        if (path.startsWith("/webdav")) {
            handleWebDavRequest(context, request, path);
            return;
        }

        if (HttpMethod.GET.equals(request.method())) {
            handleGetRequest(context, request, path, queryStringDecoder);
        } else if (HttpMethod.POST.equals(request.method())) {
            handlePostRequest(context, request, path, queryStringDecoder);
        } else {
            // 对于非 WebDAV 路径的其他方法，返回 405
            sendError(context, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    /**
     * 处理 GET 请求
     */
    private void handleGetRequest(ChannelHandlerContext context, HttpRequest request, String path,
            QueryStringDecoder queryStringDecoder) throws Exception {
        if ("/list".equals(path)) {
            handleList(context, queryStringDecoder);
        } else if ("/download".equals(path)) {
            handleDownload(context, queryStringDecoder, request);
        } else if ("/".equals(path) || "/index.html".equals(path) || "/favicon.ico".equals(path)) {
            handleStatic(context, path);
        } else {
            sendError(context, HttpResponseStatus.NOT_FOUND);
        }
    }

    /**
     * 处理 POST 请求
     */
    private void handlePostRequest(ChannelHandlerContext context, HttpRequest request, String path,
            QueryStringDecoder queryStringDecoder) throws Exception {
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
    }

    /**
     * 处理 WebDAV 请求
     */
    private void handleWebDavRequest(ChannelHandlerContext context, HttpRequest request, String path) throws Exception {
        // 移除 /webdav 前缀，得到相对路径，如 /folder/file.txt
        // 确保至少是 /
        var relativePath = path.substring(7);
        if (relativePath.isEmpty()) {
            relativePath = "/";
        }

        var method = request.method().name();
        logger.info("WebDAV {} {}", method, relativePath);

        try {
            switch (method) {
                case "OPTIONS":
                    handleWebDavOptions(context);
                    break;
                case "PROPFIND":
                    handleWebDavPropFind(context, request, relativePath);
                    break;
                case "MKCOL":
                    handleWebDavMkCol(context, relativePath);
                    break;
                case "PUT":
                    handleWebDavPut(context, request, relativePath);
                    break;
                case "GET":
                    handleWebDavGet(context, request, relativePath);
                    break;
                case "DELETE":
                    handleWebDavDelete(context, relativePath);
                    break;
                case "COPY":
                    handleWebDavCopy(context, request, relativePath);
                    break;
                case "MOVE":
                    handleWebDavMove(context, request, relativePath);
                    break;
                default:
                    sendError(context, HttpResponseStatus.NOT_IMPLEMENTED);
            }
        } catch (java.io.FileNotFoundException exception) {
            sendError(context, HttpResponseStatus.NOT_FOUND);
        } catch (Exception exception) {
            logger.error("WebDAV Error", exception);
            sendError(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }

    private void handleWebDavOptions(ChannelHandlerContext context) {
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.ALLOW, "OPTIONS, PROPFIND, MKCOL, GET, PUT, DELETE, COPY, MOVE");
        response.headers().set("DAV", "1"); // Level 1 compliance
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        context.writeAndFlush(response);
    }

    private void handleWebDavMkCol(ChannelHandlerContext context, String path) throws Exception {
        fileService.createDirectoryByPath(path);
        sendResponse(context, HttpResponseStatus.CREATED, "");
    }

    private void handleWebDavDelete(ChannelHandlerContext context, String path) throws Exception {
        var meta = fileService.resolvePath(path);
        fileService.delete(meta.getId());
        sendResponse(context, HttpResponseStatus.NO_CONTENT, "");
    }

    private void handleWebDavCopy(ChannelHandlerContext context, HttpRequest request, String path) throws Exception {
        var destination = getDestinationPath(request);
        if (destination == null) {
            sendError(context, HttpResponseStatus.BAD_REQUEST, "Destination header missing");
            return;
        }

        var overwrite = request.headers().get("Overwrite", "T").equalsIgnoreCase("T");
        var strategy = overwrite ? "OVERWRITE" : "FAIL";

        // 源文件
        var sourceMeta = fileService.resolvePath(path);
        // 目标父路径和文件名
        var destFolder = destination.substring(0, destination.lastIndexOf('/'));
        if (destFolder.isEmpty())
            destFolder = "/"; // 根目录
        // 目标父节点
        var destParentMeta = fileService.resolvePath(destFolder);

        // 如果目标父节点不是文件夹
        if (destParentMeta.getIsFolder() == 0) {
            sendError(context, HttpResponseStatus.CONFLICT, "Destination parent is not a collection");
            return;
        }

        // 目标路径如果已存在，则根据 overwrite 处理
        // FileService.copyNode 会处理重名检测
        try {
            fileService.copyNode(sourceMeta.getId(), destParentMeta.getId(), strategy);
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException e) {
            sendError(context, HttpResponseStatus.PRECONDITION_FAILED, "File or folder exists and Overwrite is F");
            return;
        } catch (cn.edu.bit.hyperfs.service.DatabaseService.MoveException e) {
            // 如：不能覆盖文件夹，或不能覆盖文件到自己
            sendError(context, HttpResponseStatus.CONFLICT, e.getMessage());
            return;
        }

        sendResponse(context, overwrite ? HttpResponseStatus.NO_CONTENT : HttpResponseStatus.CREATED, "");
    }

    private void handleWebDavMove(ChannelHandlerContext context, HttpRequest request, String path) throws Exception {
        var destination = getDestinationPath(request);
        if (destination == null) {
            sendError(context, HttpResponseStatus.BAD_REQUEST, "Destination header missing");
            return;
        }

        var overwrite = request.headers().get("Overwrite", "T").equalsIgnoreCase("T");
        var strategy = overwrite ? "OVERWRITE" : "FAIL";

        // 源文件
        var sourceMeta = fileService.resolvePath(path);

        // 解析目标：父目录 + 新文件名
        // Destination 形如 /webdav/folder/newname
        // 移除 /webdav 前缀已在 getDestinationPath 处理

        // 提取文件名和父路径
        String destParentPath;
        String destFileName;

        if (destination.equals("/")) {
            sendError(context, HttpResponseStatus.FORBIDDEN, "Cannot move to root");
            return;
        }

        // 移除末尾斜杠（如果有）
        if (destination.endsWith("/")) {
            destination = destination.substring(0, destination.length() - 1);
        }

        int lastSlash = destination.lastIndexOf('/');
        if (lastSlash >= 0) {
            destParentPath = destination.substring(0, lastSlash);
            destFileName = destination.substring(lastSlash + 1);
        } else {
            destParentPath = "/";
            destFileName = destination;
        }
        if (destParentPath.isEmpty())
            destParentPath = "/";

        var destParentMeta = fileService.resolvePath(destParentPath);

        // 如果目标父节点不是文件夹
        if (destParentMeta.getIsFolder() == 0) {
            sendError(context, HttpResponseStatus.CONFLICT, "Destination parent is not a collection");
            return;
        }

        // 判断是移动还是重命名
        // 如果父节点 ID 相同，就是重命名
        if (sourceMeta.getParentId() == destParentMeta.getId()) {
            // 重命名逻辑
            // 检查目标是否存在
            try {
                // WebDAV MOVE 要求如果 Overwrite=T 且目标存在，则覆盖（删除目标）
                // renameNode 这里只实现了简单重命名，我们需要根据 Overwrite 处理冲突
                // 实际上 FileService.moveNode 已经包含了更完善的冲突处理逻辑
                // 即使是同一目录，moveNode 也能工作（虽然有点重，但逻辑一致）
                fileService.moveNode(sourceMeta.getId(), destParentMeta.getId(), destFileName, strategy);
            } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException e) {
                sendError(context, HttpResponseStatus.PRECONDITION_FAILED, "File exists and Overwrite is F");
                return;
            } catch (cn.edu.bit.hyperfs.service.DatabaseService.MoveException e) {
                sendError(context, HttpResponseStatus.CONFLICT, e.getMessage());
                return;
            }
        } else {
            // 移动到不同目录
            try {
                fileService.moveNode(sourceMeta.getId(), destParentMeta.getId(), destFileName, strategy);
            } catch (cn.edu.bit.hyperfs.service.DatabaseService.FileConflictException e) {
                sendError(context, HttpResponseStatus.PRECONDITION_FAILED, "File exists and Overwrite is F");
                return;
            } catch (cn.edu.bit.hyperfs.service.DatabaseService.MoveException e) {
                sendError(context, HttpResponseStatus.CONFLICT, e.getMessage());
                return;
            }
        }

        sendResponse(context, HttpResponseStatus.CREATED, ""); // 或者 NO_CONTENT
    }

    private String getDestinationPath(HttpRequest request) {
        var destHeader = request.headers().get("Destination");
        if (destHeader == null) {
            return null;
        }
        // Destination 是绝对 URI，如 http://localhost:14514/webdav/folder/file.txt
        // 或者是相对 URI
        try {
            var uri = new java.net.URI(destHeader);
            var path = uri.getPath(); // 获取路径部分，已解码? URI.getPath() returns decoded path component? NO, it returns
                                      // valid path chars.
            // 实际上 Netty 的 uri 是字符串。URI.create(s).getPath() 会解码吗？
            // 测试表明 URI.getPath() 会解码 %20 但可能保留其他。
            // 此外，我们需要处理 /webdav 前缀
            // 假设服务根是 /webdav

            // 让我们自己简单处理一下 decoding，以防万一
            path = java.net.URLDecoder.decode(path, StandardCharsets.UTF_8);

            if (path.startsWith("/webdav")) {
                path = path.substring(7);
            }
            if (path.isEmpty())
                return "/";
            return path;
        } catch (Exception e) {
            logger.error("Failed to parse Destination header", e);
            return null;
        }
    }

    private void handleWebDavPut(ChannelHandlerContext context, HttpRequest request, String path) throws Exception {
        // WebDAV PUT 通常直接在 Body 中包含文件内容
        // 这里需要状态机来接收 Content。由于 Netty 是异步的，我们需要设置状态
        // 为了简化，我们需要修改 ChannelRead0 的逻辑，或者在这里初始化上传 session

        // 简单实现：仅支持小文件一次性传输，或者复用 uploadSession 逻辑
        // 但 WebDAV PUT 没有 multi-part，是 raw body。
        // 我们需要解析 Path 得到 parentId 和 filename

        if (path.endsWith("/")) {
            sendError(context, HttpResponseStatus.BAD_REQUEST, "Cannot PUT to a directory");
            return;
        }

        int lastSlash = path.lastIndexOf('/');
        var parentPath = (lastSlash > 0) ? path.substring(0, lastSlash) : "/";
        var filename = path.substring(lastSlash + 1);

        var parentMeta = fileService.resolvePath(parentPath);
        if (parentMeta.getIsFolder() != 1) {
            sendError(context, HttpResponseStatus.CONFLICT, "Parent is not a folder");
            return;
        }

        // 初始化上传
        startUploadSession(parentMeta.getId(), filename);

        // 如果 request 也是 FullHttpRequest，包含初始内容
        if (request instanceof FullHttpRequest fullRequest) {
            uploadSession.processChunk(fullRequest.content());
            fileService.completeUpload(uploadSession, currentParentId, currentFilename);
            uploadSession = null;
            isUploading = false;
            sendResponse(context, HttpResponseStatus.CREATED, "");
        }
        // 否则等待后续 HttpContent
    }

    private void handleWebDavGet(ChannelHandlerContext context, HttpRequest request, String path) throws Exception {
        var meta = fileService.resolvePath(path);
        if (meta.getIsFolder() == 1) {
            // GET 文件夹通常不被支持，或者返回 HTML 列表
            // WebDAV 客户端通常用 PROPFIND 浏览目录
            sendError(context, HttpResponseStatus.BAD_REQUEST, "Cannot GET a directory");
            return;
        }

        // 复用 handleDownload 逻辑
        // 需要构建一个假的 download 流程，或者提取 handleDownload 的公共部分
        // 鉴于 handleDownload 依赖 query parameter "id"，我们这里需要手动查找
        // 我们可以为 QueryStringDecoder 注入 id 参数，或者重构 handleDownload。
        // 最简单的：直接调用底层的 fileService.startDownload 既然我们已经有了 ID

        var id = meta.getId();

        // 以下逻辑复用 handleDownload 的核心部分
        // 获取全量资源以确认文件存在并获取总大小
        var downloadResource = fileService.startDownload(id, 0, Long.MAX_VALUE);
        var file = downloadResource.file();
        var totalLength = downloadResource.totalLength();

        // 解析 Range
        var range = parseRange(request.headers().get(HttpHeaderNames.RANGE), totalLength);

        // ... (省略部分重复逻辑，实际上应该提取公共方法 sendFileResponse)
        // 为了避免代码重复，建议将 handleDownload 中的发送逻辑提取为 sendFileResponse(ctx, req, resource,
        // range)

        // 简单起见，这里完整复制一次逻辑
        if (range.isPartial()) {
            if (range.start() < 0 || range.end() >= totalLength || range.start() > range.end()) {
                downloadResource.region().release();
                var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                        HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
                response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes */" + totalLength);
                context.writeAndFlush(response);
                return;
            }
        }
        long contentLength = range.end() - range.start() + 1;
        DefaultFileRegion finalRegion;
        if (range.isPartial()) {
            downloadResource.region().release();
            finalRegion = new DefaultFileRegion(file, range.start(), contentLength);
        } else {
            finalRegion = downloadResource.region();
        }

        var status = range.isPartial() ? HttpResponseStatus.PARTIAL_CONTENT : HttpResponseStatus.OK;
        var response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, contentLength);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
        response.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");
        // WebDAV GET 不需要 Content-Disposition attachment，因为它被视为直接访问
        // response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION, "attachment;
        // ...");

        if (range.isPartial()) {
            response.headers().set(HttpHeaderNames.CONTENT_RANGE,
                    "bytes " + range.start() + "-" + range.end() + "/" + totalLength);
        }
        if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        context.write(response);
        var future = context.writeAndFlush(finalRegion, context.newProgressivePromise());
        if (!HttpUtil.isKeepAlive(request)) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void handleWebDavPropFind(ChannelHandlerContext context, HttpRequest request, String path)
            throws Exception {
        // PROPFIND
        var depthHeader = request.headers().get("Depth");
        int depth = (depthHeader != null) ? Integer.parseInt(depthHeader) : 1; // Default "1" (infinite is costly)

        var targetMeta = fileService.resolvePath(path);

        // 构建 MultiStatus
        var multiStatus = new MultiStatus();

        // 添加目标自身
        addResponse(multiStatus, targetMeta, path);

        // 如果是文件夹且 depth > 0，添加子节点
        if (targetMeta.getIsFolder() == 1 && depth > 0) {
            var children = fileService.list(targetMeta.getId());
            for (var child : children) {
                // 构造子路径
                var childPath = path;
                if (!childPath.endsWith("/"))
                    childPath += "/";
                childPath += child.getName();
                addResponse(multiStatus, child, childPath);
            }
        }

        // 序列化 XML
        var xmlBytes = xmlMapper.writeValueAsBytes(multiStatus);
        logger.debug("PROPFIND response XML: {}", new String(xmlBytes, StandardCharsets.UTF_8));

        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.MULTI_STATUS,
                io.netty.buffer.Unpooled.wrappedBuffer(xmlBytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml; charset=utf-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, xmlBytes.length);
        if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
        context.writeAndFlush(response);
    }

    /**
     * URL 编码 WebDAV 路径（逐段编码）
     * 
     * @param path 原始路径
     * @return URL 编码后的路径
     */
    private String encodePathForWebDav(String path) {
        if (path == null || path.isEmpty()) {
            return "/";
        }
        var segments = path.split("/");
        var encodedSegments = new java.util.ArrayList<String>();
        for (var segment : segments) {
            if (!segment.isEmpty()) {
                try {
                    // 编码每个路径段，然后将 + 替换为 %20（因为 URLEncoder 会将空格编码为 +）
                    var encoded = java.net.URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
                    encodedSegments.add(encoded);
                } catch (Exception e) {
                    encodedSegments.add(segment);
                }
            }
        }
        return "/" + String.join("/", encodedSegments);
    }

    private void addResponse(MultiStatus multiStatus, cn.edu.bit.hyperfs.entity.FileMetaEntity meta, String path) {
        var response = new DavResponse();
        // href 必须是 URL 编码的（逐段编码）
        var encodedPath = encodePathForWebDav(path);
        var href = "/webdav" + (encodedPath.startsWith("/") ? encodedPath : "/" + encodedPath);

        response.setHref(href);

        var propStat = new PropStat();
        propStat.setStatus("HTTP/1.1 200 OK");

        var prop = new Prop();
        prop.setDisplayname(meta.getName());

        if (meta.getIsFolder() == 1) {
            prop.setResourcetype(new ResourceType(new Collection()));
        } else {
            prop.setResourcetype(new ResourceType(null));
            prop.setGetcontentlength(String.valueOf(meta.getSize()));
        }

        // 格式化时间
        // getlastmodified: RFC 1123 (e.g., Sun, 06 Nov 1994 08:49:37 GMT)
        // creationdate: ISO 8601 (e.g., 1994-11-06T08:49:37Z)

        // 假设 meta.getUploadTime() 是 ISO 8601 字符串或时间戳
        // 这里需要解析并重新格式化。为了简单，我们假设数据库存储的是 UTC 时间字符串。
        // 如果是时间戳，需转换。假设是 "yyyy-MM-dd HH:mm:ss"
        try {
            // 使用自定义格式化器确保两位数日期 (RFC 1123 strict)
            var rfc1123Formatter = java.time.format.DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z",
                    java.util.Locale.US);
            var now = java.time.ZonedDateTime.now(java.time.ZoneId.of("GMT"));
            prop.setGetlastmodified(now.format(rfc1123Formatter));
            prop.setCreationdate(now.toInstant().toString()); // ISO 8601 格式
        } catch (Exception e) {
            logger.warn("Failed to format date", e);
        }

        propStat.setProp(prop);
        response.setPropstat(propStat);

        multiStatus.addResponse(response);
    }

    private void handleHttpContent(ChannelHandlerContext context, HttpContent content) throws Exception {
        if (isUploading && uploadSession != null) {
            uploadSession.processChunk(content.content());

            if (content instanceof LastHttpContent) {
                fileService.completeUpload(uploadSession, currentParentId, currentFilename);
                uploadSession = null;
                isUploading = false;

                // 区分 WebDAV 和 普通上传
                // 普通上传 handleUploadStart 返回的是 OK and Text body
                // WebDAV PUT 返回 Created context is handled inside handleWebDavPut?
                // 上面的 `handleWebDavPut` 如果是 FullHttpRequest 其实已经处理完了。
                // 如果是分块的，这里处理完 LastHttpContent 应该返回响应
                sendResponse(context, HttpResponseStatus.CREATED, "");
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

    // --- WebDAV XML Structures ---

    @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement(localName = "multistatus", namespace = "DAV:")
    static class MultiStatus {
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper(useWrapping = false)
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(localName = "response", namespace = "DAV:")
        private java.util.List<DavResponse> responses = new java.util.ArrayList<>();

        public void addResponse(DavResponse response) {
            responses.add(response);
        }

        public java.util.List<DavResponse> getResponses() {
            return responses;
        }
    }

    static class DavResponse {
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private String href;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private PropStat propstat;

        public void setHref(String href) {
            this.href = href;
        }

        public String getHref() {
            return href;
        }

        public void setPropstat(PropStat propstat) {
            this.propstat = propstat;
        }

        public PropStat getPropstat() {
            return propstat;
        }
    }

    static class PropStat {
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private Prop prop;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private String status;

        public void setProp(Prop prop) {
            this.prop = prop;
        }

        public Prop getProp() {
            return prop;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    static class Prop {
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private String displayname;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private ResourceType resourcetype;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private String getcontentlength;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private String getlastmodified;

        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private String creationdate;

        public void setDisplayname(String name) {
            this.displayname = name;
        }

        public String getDisplayname() {
            return displayname;
        }

        public void setResourcetype(ResourceType type) {
            this.resourcetype = type;
        }

        public ResourceType getResourcetype() {
            return resourcetype;
        }

        public void setGetcontentlength(String len) {
            this.getcontentlength = len;
        }

        public String getGetcontentlength() {
            return getcontentlength;
        }

        public void setGetlastmodified(String date) {
            this.getlastmodified = date;
        }

        public String getGetlastmodified() {
            return getlastmodified;
        }

        public void setCreationdate(String date) {
            this.creationdate = date;
        }

        public String getCreationdate() {
            return creationdate;
        }
    }

    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    static class ResourceType {
        @com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty(namespace = "DAV:")
        private Collection collection;

        public ResourceType(Collection collection) {
            this.collection = collection;
        }

        public Collection getCollection() {
            return collection;
        }
    }

    static class Collection {
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

        sendDownloadResponse(context, request, id);
    }

    /**
     * 发送下载响应（断点续传核心逻辑）
     * 
     * @param context ChannelHandlerContext
     * @param request HttpRequest
     * @param id      文件ID
     */
    private void sendDownloadResponse(ChannelHandlerContext context, HttpRequest request, long id) throws Exception {
        try {
            // 获取全量资源以确认文件存在并获取总大小
            var downloadResource = fileService.startDownload(id, 0, Long.MAX_VALUE);
            var file = downloadResource.file();
            var totalLength = downloadResource.totalLength();
            var filename = downloadResource.filename();

            // 解析 Range 头
            var range = parseRange(request.headers().get(HttpHeaderNames.RANGE), totalLength);

            // 校验 Range 有效性
            if (range.isPartial()) {
                if (range.start() < 0 || range.end() >= totalLength || range.start() > range.end()) {
                    // Range 不满足，释放并返回 416
                    downloadResource.region().release();
                    var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                            HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
                    response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes */" + totalLength);
                    context.writeAndFlush(response);
                    return;
                }
            }

            // 根据需要调整下载区域
            long contentLength = range.end() - range.start() + 1;
            DefaultFileRegion finalRegion;
            if (range.isPartial()) {
                // 释放原始全量资源，创建新的部分资源
                downloadResource.region().release();
                finalRegion = new DefaultFileRegion(file, range.start(), contentLength);
            } else {
                finalRegion = downloadResource.region();
            }

            // 构建 HTTP 响应
            var status = range.isPartial() ? HttpResponseStatus.PARTIAL_CONTENT : HttpResponseStatus.OK;
            var response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, contentLength);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
            response.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");

            // URL 编码文件名
            var encodedFilename = java.net.URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");
            response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION,
                    "attachment; filename*=UTF-8''" + encodedFilename);

            if (range.isPartial()) {
                response.headers().set(HttpHeaderNames.CONTENT_RANGE,
                        "bytes " + range.start() + "-" + range.end() + "/" + totalLength);
            }

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            context.write(response);
            var future = context.writeAndFlush(finalRegion, context.newProgressivePromise());

            // 如果非 Keep-Alive 则关闭连接
            if (!HttpUtil.isKeepAlive(request)) {
                future.addListener(ChannelFutureListener.CLOSE);
            }

        } catch (Exception exception) {
            logger.error("Download failed", exception);
            sendError(context, HttpResponseStatus.NOT_FOUND, exception.getMessage());
        }
    }

    /**
     * 解析 Range 头
     */
    private Range parseRange(String rangeHeader, long totalLength) {
        long start = 0;
        long end = totalLength - 1;
        boolean isPartial = false;

        if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
            try {
                var rangeValue = rangeHeader.substring(6).trim();
                if (rangeValue.startsWith("-")) {
                    // bytes=-y (最后 y 个字节)
                    var suffixLength = Long.parseLong(rangeValue.substring(1));
                    start = Math.max(0, totalLength - suffixLength);
                } else {
                    // bytes=x-y 或 bytes=x-
                    var parts = rangeValue.split("-");
                    start = Long.parseLong(parts[0]);
                    if (parts.length > 1 && !parts[1].isEmpty()) {
                        end = Long.parseLong(parts[1]);
                    }
                }
                isPartial = true;
            } catch (NumberFormatException exception) {
                // 忽略格式错误，降级为普通下载
                logger.warn("Invalid range header: {}", rangeHeader);
                isPartial = false;
                start = 0;
                end = totalLength - 1;
            }
        }
        return new Range(start, end, isPartial);
    }

    private record Range(long start, long end, boolean isPartial) {
    }

    private void handleUploadStart(ChannelHandlerContext context, QueryStringDecoder queryStringDecoder)
            throws Exception {
        var parentId = getLongParam(queryStringDecoder, "parentId", 0);
        var filename = getStringParam(queryStringDecoder, "filename", DEFAULT_FILENAME);

        startUploadSession(parentId, filename);
        // 不发送响应，等待数据传输完成
    }

    private void startUploadSession(long parentId, String filename) throws Exception {
        this.currentParentId = parentId;
        this.currentFilename = filename;
        this.uploadSession = fileService.startUpload();
        this.isUploading = true;
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
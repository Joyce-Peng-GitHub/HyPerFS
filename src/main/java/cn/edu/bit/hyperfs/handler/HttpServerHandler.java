package cn.edu.bit.hyperfs.handler;

import cn.edu.bit.hyperfs.entity.FileMetaEntity;
import cn.edu.bit.hyperfs.service.DatabaseService;
import cn.edu.bit.hyperfs.service.FileService;
import cn.edu.bit.hyperfs.service.FileUploadSession;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * HTTP 服务处理器
 * 
 * 详细描述：
 * 处理所有入站 HTTP 请求，包括 REST API 和 WebDAV 协议。
 * 负责请求路由、参数解析、调用 Service 层逻辑以及构造 HTTP 响应。
 */
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
    private static final XmlMapper xmlMapper = new XmlMapper();

    public HttpServerHandler() {
        super();
        xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        xmlMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
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

        if (HttpMethod.GET.equals(request.method()) || HttpMethod.HEAD.equals(request.method())) {
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
     *
     * @param context ChannelHandlerContext
     * @param request HTTP请求
     * @param path    路径
     * @throws Exception 异常
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
        } catch (FileNotFoundException exception) {
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

    /**
     * 处理 WebDAV MKCOL 请求 (创建集合/文件夹)
     *
     * @param context ChannelHandlerContext
     * @param path    路径
     * @throws Exception 异常
     */
    private void handleWebDavMkCol(ChannelHandlerContext context, String path) throws Exception {
        fileService.createDirectoryByPath(path);
        sendResponse(context, HttpResponseStatus.CREATED, "");
    }

    /**
     * 处理 WebDAV DELETE 请求
     *
     * @param context ChannelHandlerContext
     * @param path    路径
     * @throws Exception 异常
     */
    private void handleWebDavDelete(ChannelHandlerContext context, String path) throws Exception {
        var meta = fileService.resolvePath(path);
        fileService.delete(meta.getId());
        sendResponse(context, HttpResponseStatus.NO_CONTENT, "");
    }

    /**
     * 处理 WebDAV COPY 请求
     *
     * @param context ChannelHandlerContext
     * @param request HTTP请求
     * @param path    源路径
     * @throws Exception 异常
     */
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
        } catch (DatabaseService.FileConflictException e) {
            sendError(context, HttpResponseStatus.PRECONDITION_FAILED, "File or folder exists and Overwrite is F");
            return;
        } catch (DatabaseService.MoveException e) {
            // 如：不能覆盖文件夹，或不能覆盖文件到自己
            sendError(context, HttpResponseStatus.CONFLICT, e.getMessage());
            return;
        }

        sendResponse(context, overwrite ? HttpResponseStatus.NO_CONTENT : HttpResponseStatus.CREATED, "");
    }

    /**
     * 处理 WebDAV MOVE 请求
     *
     * @param context ChannelHandlerContext
     * @param request HTTP请求
     * @param path    源路径
     * @throws Exception 异常
     */
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
            } catch (DatabaseService.FileConflictException e) {
                sendError(context, HttpResponseStatus.PRECONDITION_FAILED, "File exists and Overwrite is F");
                return;
            } catch (DatabaseService.MoveException e) {
                sendError(context, HttpResponseStatus.CONFLICT, e.getMessage());
                return;
            }
        } else {
            // 移动到不同目录
            try {
                fileService.moveNode(sourceMeta.getId(), destParentMeta.getId(), destFileName, strategy);
            } catch (DatabaseService.FileConflictException e) {
                sendError(context, HttpResponseStatus.PRECONDITION_FAILED, "File exists and Overwrite is F");
                return;
            } catch (DatabaseService.MoveException e) {
                sendError(context, HttpResponseStatus.CONFLICT, e.getMessage());
                return;
            }
        }

        sendResponse(context, HttpResponseStatus.CREATED, ""); // 或者 NO_CONTENT
    }

    /**
     * 获取 WebDAV Destination 头并解析路径
     *
     * @param request HTTP请求
     * @return 目标路径（相对）
     */
    private String getDestinationPath(HttpRequest request) {
        var destHeader = request.headers().get("Destination");
        if (destHeader == null) {
            return null;
        }
        // Destination 是绝对 URI，如 http://localhost:14514/webdav/folder/file.txt
        // 或者是相对 URI
        try {
            var uri = new URI(destHeader);

            // 获取路径部分
            // URI.getPath() 返回解码后的路径组件
            var path = uri.getPath();

            // 再次进行 URL 解码以确保所有特殊字符（如中文）被正确处理
            // (某些客户端可能对 Path 处理不一致)
            path = URLDecoder.decode(path, StandardCharsets.UTF_8);

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

    /**
     * 处理 WebDAV PUT 请求 (文件上传)
     *
     * @param context ChannelHandlerContext
     * @param request HTTP请求
     * @param path    路径
     * @throws Exception 异常
     */
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
        sendDownloadResponse(context, request, meta.getId(), false);
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
        var encodedSegments = new ArrayList<String>();
        for (var segment : segments) {
            if (!segment.isEmpty()) {
                try {
                    // 编码每个路径段，然后将 + 替换为 %20（因为 URLEncoder 会将空格编码为 +）
                    var encoded = URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
                    encodedSegments.add(encoded);
                } catch (Exception e) {
                    encodedSegments.add(segment);
                }
            }
        }
        return "/" + String.join("/", encodedSegments);
    }

    /**
     * 添加 WebDAV 响应到 MultiStatus
     *
     * @param multiStatus MultiStatus 对象
     * @param meta        文件元数据
     * @param path        文件路径
     */
    private void addResponse(MultiStatus multiStatus, FileMetaEntity meta, String path) {
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
        try {
            // 使用自定义格式化器确保两位数日期 (RFC 1123 strict)
            var rfc1123Formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z",
                    Locale.US);
            var now = ZonedDateTime.now(ZoneId.of("GMT"));
            prop.setGetlastmodified(now.format(rfc1123Formatter));
            prop.setCreationdate(now.toInstant().toString()); // ISO 8601 格式
        } catch (Exception e) {
            logger.warn("Failed to format date", e);
        }

        propStat.setProp(prop);
        response.setPropstat(propStat);

        multiStatus.addResponse(response);
    }

    /**
     * 处理 HTTP 内容 (Body)
     *
     * @param context ChannelHandlerContext
     * @param content HttpContent
     * @throws Exception 异常
     */
    private void handleHttpContent(ChannelHandlerContext context, HttpContent content) throws Exception {
        if (isUploading && uploadSession != null) {
            uploadSession.processChunk(content.content());

            if (content instanceof LastHttpContent) {
                fileService.completeUpload(uploadSession, currentParentId, currentFilename);
                uploadSession = null;
                isUploading = false;

                // 区分 WebDAV 和 普通上传
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

    @JacksonXmlRootElement(localName = "multistatus", namespace = "DAV:")
    static class MultiStatus {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "response", namespace = "DAV:")
        private List<DavResponse> responses = new ArrayList<>();

        public void addResponse(DavResponse response) {
            responses.add(response);
        }

        public List<DavResponse> getResponses() {
            return responses;
        }
    }

    static class DavResponse {
        @JacksonXmlProperty(namespace = "DAV:")
        private String href;

        @JacksonXmlProperty(namespace = "DAV:")
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
        @JacksonXmlProperty(namespace = "DAV:")
        private Prop prop;

        @JacksonXmlProperty(namespace = "DAV:")
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    static class Prop {
        @JacksonXmlProperty(namespace = "DAV:")
        private String displayname;

        @JacksonXmlProperty(namespace = "DAV:")
        private ResourceType resourcetype;

        @JacksonXmlProperty(namespace = "DAV:")
        private String getcontentlength;

        @JacksonXmlProperty(namespace = "DAV:")
        private String getlastmodified;

        @JacksonXmlProperty(namespace = "DAV:")
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    static class ResourceType {
        @JacksonXmlProperty(namespace = "DAV:")
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

        sendDownloadResponse(context, request, id, true);
    }

    /**
     * 发送下载响应（断点续传核心逻辑）
     * 
     * @param context      ChannelHandlerContext
     * @param request      HttpRequest
     * @param id           文件ID
     * @param asAttachment 是否作为附件下载 (WebDAV GET 通常为 false)
     */
    private void sendDownloadResponse(ChannelHandlerContext context, HttpRequest request, long id, boolean asAttachment)
            throws Exception {
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

            if (asAttachment) {
                // URL 编码文件名
                var encodedFilename = URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");
                response.headers().set(HttpHeaderNames.CONTENT_DISPOSITION,
                        "attachment; filename*=UTF-8''" + encodedFilename);
            }

            if (range.isPartial()) {
                response.headers().set(HttpHeaderNames.CONTENT_RANGE,
                        "bytes " + range.start() + "-" + range.end() + "/" + totalLength);
            }

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            context.write(response);

            if (HttpMethod.HEAD.equals(request.method())) {
                // HEAD 请求不发送 Body，但由于我们使用了 FileRegion，需要释放它
                if (finalRegion.refCnt() > 0) {
                    finalRegion.release();
                }
            } else {
                var future = context.writeAndFlush(finalRegion, context.newProgressivePromise());
                // 如果非 Keep-Alive 则关闭连接
                if (!HttpUtil.isKeepAlive(request)) {
                    future.addListener(ChannelFutureListener.CLOSE);
                }
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
        // 返回 index.html
        try (var inputStream = getClass().getResourceAsStream("/static/index.html")) {
            if (inputStream == null) {
                sendError(context, HttpResponseStatus.NOT_FOUND, "index.html not found");
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
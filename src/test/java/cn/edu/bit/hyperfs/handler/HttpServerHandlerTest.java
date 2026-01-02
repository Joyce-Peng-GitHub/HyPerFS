package cn.edu.bit.hyperfs.handler;

import cn.edu.bit.hyperfs.service.FileDownloadResource;
import cn.edu.bit.hyperfs.service.FileService;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HttpServerHandlerTest {

    @Mock
    private FileService fileService;

    private HttpServerHandler handler;
    private EmbeddedChannel channel;

    @BeforeEach
    public void setup() throws Exception {
        handler = new HttpServerHandler();

        // Use Reflection to inject the mock FileService into the private final field
        Field fileServiceField = HttpServerHandler.class.getDeclaredField("fileService");
        fileServiceField.setAccessible(true);
        fileServiceField.set(handler, fileService);

        channel = new EmbeddedChannel(handler);
    }

    @Test
    public void testHandleDownload_NoRange() throws Exception {
        // Setup
        long fileId = 1L;
        long fileSize = 1000L;
        File mockFile = new File("test.txt");
        // Create a real DefaultFileRegion (it interacts with File object but doesn't
        // check IO in constructor)
        DefaultFileRegion region = new DefaultFileRegion(mockFile, 0, fileSize);

        FileDownloadResource resource = new FileDownloadResource(
                region,
                "test.txt",
                fileSize,
                mockFile);

        when(fileService.startDownload(eq(fileId), anyLong(), anyLong())).thenReturn(resource);

        // Execute
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/download?id=" + fileId);
        channel.writeInbound(request);

        // Verify Response
        HttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals(String.valueOf(fileSize), response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("application/octet-stream", response.headers().get(HttpHeaderNames.CONTENT_TYPE));

        // Verify Content (FileRegion)
        Object content = channel.readOutbound();
        assertTrue(content instanceof DefaultFileRegion);
        DefaultFileRegion outRegion = (DefaultFileRegion) content;
        assertEquals(fileSize, outRegion.count());
    }

    @Test
    public void testHandleDownload_ValidRange() throws Exception {
        // Setup
        long fileId = 1L;
        long fileSize = 1000L;
        File mockFile = new File("test.txt");
        DefaultFileRegion region = new DefaultFileRegion(mockFile, 0, fileSize);

        FileDownloadResource resource = new FileDownloadResource(
                region,
                "test.txt",
                fileSize,
                mockFile);

        when(fileService.startDownload(eq(fileId), anyLong(), anyLong())).thenReturn(resource);

        // Execute Range: bytes=0-99 (100 bytes)
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/download?id=" + fileId);
        request.headers().set(HttpHeaderNames.RANGE, "bytes=0-99");
        channel.writeInbound(request);

        // Verify Response
        HttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.PARTIAL_CONTENT, response.status());
        assertEquals("100", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("bytes 0-99/1000", response.headers().get(HttpHeaderNames.CONTENT_RANGE));

        // Verify Content
        Object content = channel.readOutbound();
        assertTrue(content instanceof DefaultFileRegion);
        DefaultFileRegion outRegion = (DefaultFileRegion) content;
        assertEquals(100, outRegion.count());
        assertEquals(0, outRegion.position());
    }

    @Test
    public void testHandleDownload_SuffixRange() throws Exception {
        // Setup
        long fileId = 1L;
        long fileSize = 1000L;
        File mockFile = new File("test.txt");
        DefaultFileRegion region = new DefaultFileRegion(mockFile, 0, fileSize);

        FileDownloadResource resource = new FileDownloadResource(
                region,
                "test.txt",
                fileSize,
                mockFile);

        when(fileService.startDownload(eq(fileId), anyLong(), anyLong())).thenReturn(resource);

        // Execute Range: bytes=-100 (Last 100 bytes: 900-999)
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/download?id=" + fileId);
        request.headers().set(HttpHeaderNames.RANGE, "bytes=-100");
        channel.writeInbound(request);

        // Verify Response
        HttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.PARTIAL_CONTENT, response.status());
        assertEquals("100", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("bytes 900-999/1000", response.headers().get(HttpHeaderNames.CONTENT_RANGE));

        // Verify Content
        Object content = channel.readOutbound();
        assertTrue(content instanceof DefaultFileRegion);
        DefaultFileRegion outRegion = (DefaultFileRegion) content;
        assertEquals(100, outRegion.count());
        assertEquals(900, outRegion.position());
    }

    @Test
    public void testHandleDownload_InvalidRange() throws Exception {
        // Setup
        long fileId = 1L;
        long fileSize = 1000L;
        File mockFile = new File("test.txt");
        DefaultFileRegion region = new DefaultFileRegion(mockFile, 0, fileSize);

        FileDownloadResource resource = new FileDownloadResource(
                region,
                "test.txt",
                fileSize,
                mockFile);

        when(fileService.startDownload(eq(fileId), anyLong(), anyLong())).thenReturn(resource);

        // Execute Range: bytes=2000- (Out of bounds)
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/download?id=" + fileId);
        request.headers().set(HttpHeaderNames.RANGE, "bytes=2000-");
        channel.writeInbound(request);

        // Verify Response
        HttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE, response.status());
        assertEquals("bytes */1000", response.headers().get(HttpHeaderNames.CONTENT_RANGE));

        // Should not have file content
        Object content = channel.readOutbound();
        if (content instanceof LastHttpContent) {
            // It might send empty last content if FullHttpResponse wasn't used,
            // but here we sent DefaultFullHttpResponse which serves as both.
            // Let's check implementation:
            // "var response = new DefaultFullHttpResponse(...,
            // REQUESTED_RANGE_NOT_SATISFIABLE);"
            // "context.writeAndFlush(response);"
            // So EmbeddedChannel should receive just the response.
        } else {
            assertNull(content, "Should not return content for 416");
        }
    }
}

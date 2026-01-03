package cn.edu.bit.hyperfs;

import cn.edu.bit.hyperfs.service.FileService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PerformanceTest {

    private FileService fileService;
    private long testRootId;
    private final String TEST_ROOT_NAME = "PerfTestRoot";

    @BeforeAll
    public void setup() throws Exception {
        fileService = new FileService();
        // Cleanup if exists
        try {
            var meta = fileService.resolvePath("/" + TEST_ROOT_NAME);
            if (meta != null) {
                fileService.delete(meta.getId());
            }
        } catch (Exception ignored) {
        }

        // Create root
        testRootId = fileService.createFolder(0, TEST_ROOT_NAME);
        System.out.println("Test root created with ID: " + testRootId);
    }

    @AfterAll
    public void teardown() throws Exception {
        if (testRootId > 0) {
            fileService.delete(testRootId);
            System.out.println("Test root deleted.");
        }
    }

    @Test
    public void testDeepPathResolutionPerformance() throws Exception {
        System.out.println("Starting Deep Path Resolution Performance Test...");

        long currentId = testRootId;
        StringBuilder pathBuilder = new StringBuilder("/" + TEST_ROOT_NAME);
        int depth = 50;

        // Create deep hierarchy
        for (int i = 0; i < depth; i++) {
            String folderName = "depth_" + i;
            currentId = fileService.createFolder(currentId, folderName);
            pathBuilder.append("/").append(folderName);
        }

        String fullPath = pathBuilder.toString();
        System.out.println("Created deep path: " + fullPath);

        // Warmup
        for (int i = 0; i < 10; i++) {
            fileService.resolvePath(fullPath);
        }

        // Measure
        int iterations = 1000;
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            fileService.resolvePath(fullPath);
        }
        long endTime = System.nanoTime();

        double totalTimeMs = (endTime - startTime) / 1_000_000.0;
        double avgTimeMs = totalTimeMs / iterations;

        System.out.printf("Resolved depth-%d path %d times in %.2f ms. Avg: %.4f ms/op%n",
                depth, iterations, totalTimeMs, avgTimeMs);

        // Assertion: Should be reasonably fast (e.g. < 2ms per op for local DB with
        // optimized logic)
        // Note: With O(N) previously, if each folder had many files it would be slow.
        // Here we only have 1 file per folder so O(N) vs O(1) difference isn't huge
        // unless we populate folders.
        // To really test O(1) benefit, we should populate one level with many files.
    }

    @Test
    public void testLargeDirectoryPerformance() throws Exception {
        System.out.println("Starting Large Directory Performance Test...");

        long largeDirId = fileService.createFolder(testRootId, "LargeDir");
        String largeDirPath = "/" + TEST_ROOT_NAME + "/LargeDir";
        int fileCount = 1000;

        // Populate with many folders to simulate wide directory
        // This is where O(N) loop would suffer if we search for the last one.
        for (int i = 0; i < fileCount; i++) {
            fileService.createFolder(largeDirId, "sub_" + i);
        }

        // Search for the last one
        String targetPath = largeDirPath + "/sub_" + (fileCount - 1);

        // Measure
        int iterations = 100;
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            fileService.resolvePath(targetPath);
        }
        long endTime = System.nanoTime();

        double totalTimeMs = (endTime - startTime) / 1_000_000.0;
        double avgTimeMs = totalTimeMs / iterations;

        System.out.printf("Resolved path in wide directory (%d items) %d times in %.2f ms. Avg: %.4f ms/op%n",
                fileCount, iterations, totalTimeMs, avgTimeMs);
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        System.out.println("Starting Concurrent Access Test...");
        int threads = 20; // DefaultEventExecutorGroup size is 32, so 20 is safe
        int loops = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        long start = System.currentTimeMillis();

        for (int i = 0; i < threads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    String folderName = "Thread_" + threadId;
                    fileService.createFolder(testRootId, folderName);
                    for (int j = 0; j < loops; j++) {
                        fileService.resolvePath("/" + TEST_ROOT_NAME + "/" + folderName);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();

        System.out.println("Concurrent test finished in " + (end - start) + " ms");
    }
}

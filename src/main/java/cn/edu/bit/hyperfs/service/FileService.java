package cn.edu.bit.hyperfs.service;

import cn.edu.bit.hyperfs.db.InsertFileResult;
import cn.edu.bit.hyperfs.entity.FileMetaEntity;
import cn.edu.bit.hyperfs.handler.HttpServerHandler;
import io.netty.channel.DefaultFileRegion;

import java.io.File;
import java.io.IOException;

public class FileService {
    private final DatabaseService databaseService = new DatabaseService();
    private final File dataDirectory = new File(HttpServerHandler.DEFAULT_DATA_DIRECTORY);
    private final File tmpDirectory = new File(HttpServerHandler.DEFAULT_TMP_DIRECTORY);

    public FileService() {
        if (!dataDirectory.exists()) {
            dataDirectory.mkdirs();
        }
        if (!tmpDirectory.exists()) {
            tmpDirectory.mkdirs();
        }
    }

    public FileUploadSession startUpload() throws Exception {
        return new FileUploadSession(tmpDirectory);
    }

    public void completeUpload(FileUploadSession uploadSession, long parentId, String filename) throws Exception {
        try {
            uploadSession.close();
            String hash = uploadSession.getHashString();
            long size = uploadSession.getSize();

            InsertFileResult result = databaseService.insertFile(parentId, filename, hash, size);

            if (result.isDuplicated()) {
                // 如果重复，删除临时文件
                uploadSession.delete();
            } else {
                // 如果不重复，移动文件
                File targetFile = new File(dataDirectory, hash);
                try {
                    // 如果目标文件已存在（可能是因为虽然是新节点，但哈希值对应的文件内容已存在），则直接删除临时文件
                    // 这里有一个细微的逻辑：insertFile会检查hash对应的storage是否存在。如果存在，storageDao不会插入。
                    // 所以如果storage已存在，我们其实不需要移动文件，直接删除临时文件即可。
                    // 但是 databaseService.insertFile 返回的是本次插入操作的结果。

                    // 我们需要判断是否需要移动文件。
                    // 最好是用 existsByHash 检查一下，或者直接检查 targetFile 是否存在。
                    if (targetFile.exists()) {
                        uploadSession.delete();
                    } else {
                        uploadSession.rename(targetFile);
                    }
                } catch (IOException e) {
                    // 移动失败，回滚数据库
                    databaseService.revokeFileInsertion(result);
                    throw e;
                }
            }
        } catch (Exception e) {
            uploadSession.abort();
            throw e;
        }
    }

    public java.util.ArrayList<FileMetaEntity> list(long parentId) throws Exception {
        return databaseService.getList(parentId);
    }

    public DefaultFileRegion startDownload(long id, long startRange, long length) throws Exception {
        FileMetaEntity fileMeta = databaseService.getFileMeta(id);
        if (fileMeta == null || fileMeta.getIsFolder() == 1) {
            throw new IllegalArgumentException("File not found or is a folder");
        }

        databaseService.incrementDownloadCount(id);

        File file = new File(dataDirectory, fileMeta.getHash());
        if (!file.exists()) {
            throw new java.io.FileNotFoundException("Physical file missing: " + file.getAbsolutePath());
        }

        // 修正length，如果请求长度超过文件长度
        long fileLength = file.length();
        long actualLength = length;
        if (startRange + length > fileLength) {
            actualLength = fileLength - startRange;
        }

        // 使用 DefaultFileRegion 实现零拷贝
        return new DefaultFileRegion(file, startRange, actualLength);
    }

    public void delete(long id) throws Exception {
        databaseService.deleteNode(id);
    }

    public long createFolder(long parentId, String name) throws Exception {
        return databaseService.insertFolder(parentId, name);
    }
}

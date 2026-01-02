package cn.edu.bit.hyperfs.service;

import cn.edu.bit.hyperfs.entity.FileMetaEntity;
import cn.edu.bit.hyperfs.handler.HttpServerHandler;
import io.netty.channel.DefaultFileRegion;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileService {
    private static final Logger logger = LoggerFactory.getLogger(FileService.class);
    private final DatabaseService databaseService = new DatabaseService();
    private final File dataDirectory = new File(HttpServerHandler.DEFAULT_DATA_DIRECTORY);
    private final File temporaryDirectory = new File(HttpServerHandler.DEFAULT_TMP_DIRECTORY);

    public FileService() {
        if (!dataDirectory.exists()) {
            dataDirectory.mkdirs();
        }
        if (!temporaryDirectory.exists()) {
            temporaryDirectory.mkdirs();
        }
    }

    /**
     * 开始一个新的上传会话
     * 
     * 详细描述：
     * 初始化上传过程，创建一个用于存储上传数据的临时文件会话。
     *
     * @return 新的文件上传会话对象
     * @throws Exception 创建会话失败
     */
    public FileUploadSession startUpload() throws Exception {
        logger.info("Starting upload session");
        return new FileUploadSession(temporaryDirectory);
    }

    /**
     * 完成文件上传
     * 
     * 详细描述：
     * 结束上传会话，计算文件哈希，并将其保存到数据库和文件系统中。
     * 如果文件内容已存在（去重），则删除临时文件。
     * 如果是新内容，则将临时文件移动到数据目录。
     *
     * @param uploadSession 文件上传会话对象
     * @param parentId      父节点ID
     * @param fileName      文件名
     * @throws Exception IO错误或数据库异常
     */
    public void completeUpload(FileUploadSession uploadSession, long parentId, String fileName) throws Exception {
        try {
            uploadSession.close();
            var fileHash = uploadSession.getHashString();
            var fileSize = uploadSession.getSize();

            var result = databaseService.insertFile(parentId, fileName, fileHash, fileSize);

            if (result.isDuplicated()) {
                // 如果重复，删除临时文件
                uploadSession.delete();
            } else {
                // 如果不重复，移动文件
                var targetFile = new File(dataDirectory, fileHash);
                logger.info("Completing upload for file: {}, hash: {}, size: {}", fileName, fileHash, fileSize);
                try {
                    // 如果目标文件已存在（可能是因为虽然是新节点，但哈希值对应的文件内容已存在），则直接删除临时文件
                    // 尽管insertFile逻辑中新文件应该不会已存在storage中，防卫性编程
                    if (targetFile.exists()) {
                        uploadSession.delete();
                    } else {
                        uploadSession.rename(targetFile);
                    }
                } catch (IOException exception) {
                    // 移动失败，回滚数据库
                    databaseService.revokeFileInsertion(result);
                    throw exception;
                }
            }
        } catch (Exception exception) {
            uploadSession.abort();
            throw exception;
        }
    }

    /**
     * 列出目录内容
     * 
     * 详细描述：
     * 获取指定父节点下的所有文件和子文件夹列表。
     *
     * @param parentId 父节点ID
     * @return 文件元数据列表
     * @throws Exception 数据库查询错误
     */
    public java.util.ArrayList<FileMetaEntity> list(long parentId) throws Exception {
        return databaseService.getList(parentId);
    }

    /**
     * 开始下载文件
     * 
     * 详细描述：
     * 验证请求的文件是否存在且有效，增加下载计数。
     * 准备并返回一个文件下载资源，支持部分内容下载（Range请求）。
     *
     * @param id         要下载的文件ID
     * @param startRange 开始字节偏移
     * @param length     请求的长度
     * @return 文件下载资源对象
     * @throws Exception 文件不存在或IO错误
     */
    public FileDownloadResource startDownload(long id, long startRange, long length) throws Exception {
        var fileMeta = databaseService.getFileMeta(id);
        if (fileMeta == null || fileMeta.getIsFolder() == 1) {
            throw new IllegalArgumentException("File not found or is a folder");
        }

        logger.info("Starting download for file id: {}, hash: {}", id, fileMeta.getHash());
        databaseService.incrementDownloadCount(id);

        var file = new File(dataDirectory, fileMeta.getHash());
        if (!file.exists()) {
            throw new java.io.FileNotFoundException("Physical file missing: " + file.getAbsolutePath());
        }

        // 修正length，如果请求长度超过文件长度
        var fileLength = file.length();
        var actualLength = length;
        if (startRange + length > fileLength) {
            actualLength = fileLength - startRange;
        }

        // 使用 DefaultFileRegion 实现零拷贝
        return new FileDownloadResource(new DefaultFileRegion(file, startRange, actualLength), fileMeta.getName(),
                fileLength, file);
    }

    /**
     * 删除节点
     * 
     * 详细描述：
     * 删除指定的文件或文件夹。
     *
     * @param id 节点ID
     * @throws Exception 删除失败
     */
    public void delete(long id) throws Exception {
        logger.info("Deleting node: {}", id);
        databaseService.deleteNode(id);
    }

    /**
     * 创建文件夹
     * 
     * 详细描述：
     * 在指定位置创建一个新的文件夹。
     *
     * @param parentId 父节点ID
     * @param name     文件夹名称
     * @return 新文件夹的ID
     * @throws Exception 创建失败（如重名）
     */
    public long createFolder(long parentId, String name) throws Exception {
        logger.info("Creating folder: name={}, parentId={}", name, parentId);
        return databaseService.insertFolder(parentId, name);
    }

    /**
     * 移动节点
     * 
     * 详细描述：
     * 将文件或文件夹移动到新的位置，支持冲突检测和解决。
     *
     * @param id             节点ID
     * @param targetParentId 目标父节点ID
     * @param strategy       冲突解决策略
     * @throws Exception 移动失败
     */
    public void moveNode(long id, long targetParentId, String strategy) throws Exception {
        logger.info("Moving node: id={}, targetParentId={}, strategy={}", id, targetParentId, strategy);
        databaseService.moveNode(id, targetParentId, strategy);
    }

    /**
     * 重命名节点
     * 
     * 详细描述：
     * 修改文件或文件夹的名称。
     *
     * @param id      节点ID
     * @param newName 新名称
     * @throws Exception 重命名失败
     */
    public void renameNode(long id, String newName) throws Exception {
        logger.info("Renaming node: id={}, newName={}", id, newName);
        databaseService.renameNode(id, newName);
    }

    /**
     * 复制节点
     * 
     * 详细描述：
     * 复制文件或文件夹到指定位置，支持递归复制。
     *
     * @param id             节点ID
     * @param targetParentId 目标父节点ID
     * @param strategy       冲突解决策略
     * @throws Exception 复制失败
     */
    public void copyNode(long id, long targetParentId, String strategy) throws Exception {
        logger.info("Copying node: id={}, targetParentId={}, strategy={}", id, targetParentId, strategy);
        databaseService.copyNode(id, targetParentId, strategy);
    }
}

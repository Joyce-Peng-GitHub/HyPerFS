package cn.edu.bit.hyperfs.service;

import cn.edu.bit.hyperfs.db.*;
import cn.edu.bit.hyperfs.entity.FileMetaEntity;
import cn.edu.bit.hyperfs.entity.FileStorageEntity;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseService {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private final FileStorageDao fileStorageDao = new FileStorageDao();
    private final FileMetaDao fileMetaDao = new FileMetaDao();

    /**
     * 查询文件是否存在
     *
     * @param hash 哈希值
     * @param size 文件大小
     * @return 存在返回true，否则false
     * @throws SQLException          数据库错误
     * @throws IllegalStateException 如果有哈希碰撞
     */
    public boolean existsByHash(String hash, long size) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            FileStorageEntity entity = fileStorageDao.getByHash(connection, hash);
            if (entity != null) {
                if (entity.getSize() == size) {
                    return true;
                } else {
                    throw new IllegalStateException("Hash collision: " + hash);
                }
            }
            return false;
        }
    }

    /**
     * 列出目录
     *
     * @param parentId 父节点ID
     * @return 文件元数据列表
     * @throws SQLException 数据库错误
     */
    public ArrayList<FileMetaEntity> getList(long parentId) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            return fileMetaDao.getByParentId(connection, parentId);
        }
    }

    /**
     * 保存文件节点
     *
     * @param parentId 父节点ID
     * @param filename 文件名
     * @param hash     文件哈希
     * @param size     文件大小
     * @return 插入结果
     * @throws Exception 业务异常
     */
    public InsertFileResult insertFile(long parentId, String filename, String hash, long size) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false); // 开启事务
            try {
                // 检查对应目录下有没有同名节点
                FileMetaEntity existingNode = fileMetaDao.getByParentIdAndName(connection, parentId, filename);

                if (existingNode == null) {
                    // 没有同名节点
                    // 检查文件存储是否存在
                    FileStorageEntity storage = fileStorageDao.getByHash(connection, hash);
                    if (storage == null) {
                        fileStorageDao.insertFile(connection, hash, size);
                    } else {
                        fileStorageDao.incrementReferenceCount(connection, hash);
                    }

                    long time = System.currentTimeMillis();
                    long id = fileMetaDao.insertFile(connection, parentId, filename, hash, size, time);
                    connection.commit();
                    return new InsertFileResult(false, id);
                } else {
                    // 有同名节点
                    if (existingNode.getIsFolder() == 1) {
                        throw new IllegalArgumentException("Folder with same name already exists: " + filename);
                    }

                    if (existingNode.getHash().equals(hash)) {
                        // 同名且哈希相同，直接返回
                        connection.commit();
                        return new InsertFileResult(true, existingNode.getId());
                    } else {
                        // 同名但哈希不同
                        // 减少原文件的引用计数
                        fileStorageDao.decrementReferenceCount(connection, existingNode.getHash());

                        // 增加新文件的引用计数
                        FileStorageEntity storage = fileStorageDao.getByHash(connection, hash);
                        if (storage == null) {
                            fileStorageDao.insertFile(connection, hash, size);
                        } else {
                            fileStorageDao.incrementReferenceCount(connection, hash);
                        }

                        long time = System.currentTimeMillis();
                        fileMetaDao.updateById(connection, existingNode.getId(), hash, size, time);
                        connection.commit();
                        return new InsertFileResult(false, existingNode.getId());
                    }
                }
            } catch (Exception e) {
                logger.error("Error inserting file, rolling back", e);
                connection.rollback();
                throw e;
            }
        }
    }

    /**
     * 撤销插入
     *
     * @param result 插入结果
     * @throws SQLException 数据库错误
     */
    public void revokeFileInsertion(InsertFileResult result) throws SQLException {
        if (result.isDuplicated()) {
            return;
        }
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 用ID获取节点信息（主要是哈希值）
                FileMetaEntity entity = fileMetaDao.getById(connection, result.id());
                if (entity != null) {
                    fileStorageDao.decrementReferenceCount(connection, entity.getHash());
                    fileMetaDao.removeById(connection, result.id());
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    /**
     * 创建文件夹节点
     *
     * @param parentId 父文件夹ID
     * @param filename 文件夹名
     * @return 节点ID
     * @throws Exception 业务异常
     */
    public long insertFolder(long parentId, String filename) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                FileMetaEntity existingNode = fileMetaDao.getByParentIdAndName(connection, parentId, filename);
                if (existingNode != null) {
                    throw new IllegalArgumentException("Node with same name already exists: " + filename);
                }
                long time = System.currentTimeMillis();
                long id = fileMetaDao.insertFolder(connection, parentId, filename, time);
                connection.commit();
                return id;
            } catch (Exception e) {
                logger.error("Error inserting folder, rolling back", e);
                connection.rollback();
                throw e;
            }
        }
    }

    /**
     * 删除节点
     *
     * @param id 节点ID
     * @throws SQLException 数据库错误
     */
    public void deleteNode(long id) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                deleteNodeRecursive(connection, id);
                connection.commit();
            } catch (Exception e) {
                logger.error("Error deleting node, rolling back", e);
                connection.rollback();
                throw e;
            }
        }
    }

    private void deleteNodeRecursive(java.sql.Connection connection, long id) throws SQLException {
        FileMetaEntity entity = fileMetaDao.getById(connection, id);
        if (entity == null) {
            return;
        }

        if (entity.getIsFolder() == 1) {
            // 递归删除子节点
            var children = fileMetaDao.getByParentId(connection, id);
            for (var child : children) {
                deleteNodeRecursive(connection, child.getId());
            }
        } else {
            // 文件，减少引用计数
            fileStorageDao.decrementReferenceCount(connection, entity.getHash());
        }
        fileMetaDao.removeById(connection, id);
    }

    /**
     * 增加下载次数
     * 
     * @param id 节点ID
     * @throws SQLException 数据库错误
     */
    public void incrementDownloadCount(long id) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            fileMetaDao.incrementDownloadCountById(connection, id);
        }
    }

    /**
     * 获取节点信息
     *
     * @param id 节点ID
     * @return FileMetaEntity 或 null
     * @throws SQLException 数据库错误
     */
    public FileMetaEntity getFileMeta(long id) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            return fileMetaDao.getById(connection, id);
        }
    }

    /**
     * Move Node Exception
     */
    public static class MoveException extends Exception {
        public MoveException(String message) {
            super(message);
        }
    }

    /**
     * File already exists exception (for 409 conflict)
     */
    public static class FileConflictException extends MoveException {
        public FileConflictException(String message) {
            super(message);
        }
    }

    /**
     * Move a node to a new parent
     *
     * @param id             Node ID to move
     * @param targetParentId Target parent ID
     * @param strategy       Conflict resolution strategy: "FAIL", "RENAME",
     *                       "OVERWRITE"
     * @throws Exception error
     */
    public void moveNode(long id, long targetParentId, String strategy) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 1. Check if source exists
                FileMetaEntity source = fileMetaDao.getById(connection, id);
                if (source == null) {
                    throw new MoveException("Source node not found: " + id);
                }

                if (source.getParentId() == targetParentId) {
                    throw new MoveException("Source and destination folders are the same.");
                }

                // 2. Check if target folder exists (if not root)
                if (targetParentId != 0) {
                    FileMetaEntity targetParent = fileMetaDao.getById(connection, targetParentId);
                    if (targetParent == null) {
                        throw new MoveException("Target folder not found: " + targetParentId);
                    }
                    if (targetParent.getIsFolder() == 0) {
                        throw new MoveException("Target is not a folder: " + targetParentId);
                    }
                }

                // 3. Cycle detection (if source is folder)
                if (source.getIsFolder() == 1) {
                    long current = targetParentId;
                    while (current != 0) {
                        if (current == id) {
                            throw new MoveException("Cannot move a node into its own child");
                        }
                        FileMetaEntity parent = fileMetaDao.getById(connection, current);
                        if (parent == null)
                            break;
                        current = parent.getParentId();
                    }
                }

                String finalName = source.getName();

                // 4. Check for conflict
                FileMetaEntity conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId,
                        source.getName());
                if (conflict != null) {
                    // Handle conflict based on strategy
                    if ("RENAME".equalsIgnoreCase(strategy)) {
                        // Find available name: name (N)
                        String baseName = source.getName();
                        String nameWithoutExt = baseName;
                        String ext = "";
                        int lastDot = baseName.lastIndexOf('.');
                        if (lastDot > 0) {
                            nameWithoutExt = baseName.substring(0, lastDot);
                            ext = baseName.substring(lastDot);
                        }

                        int i = 1;
                        while (conflict != null) {
                            finalName = nameWithoutExt + " (" + i + ")" + ext;
                            conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId, finalName);
                            i++;
                        }
                    } else if ("OVERWRITE".equalsIgnoreCase(strategy)) {
                        // Check if overwritable
                        if (source.getIsFolder() == 1 || conflict.getIsFolder() == 1) {
                            throw new MoveException("Cannot overwrite folder or with folder");
                        }
                        // Delete conflict
                        deleteNodeRecursive(connection, conflict.getId());
                    } else {
                        // FAIL (Default)
                        throw new FileConflictException(
                                "File with the same name already exists in the target directory.");
                    }
                }

                // 5. Execute Move
                fileMetaDao.updateParentIdAndName(connection, id, targetParentId, finalName);

                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    public void renameNode(long id, String newName) throws SQLException, MoveException {
        try (Connection connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                FileMetaEntity node = fileMetaDao.getById(connection, id);
                if (node == null) {
                    throw new MoveException("Node not found");
                }

                if (node.getName().equals(newName)) {
                    return; // No change
                }

                // Check for conflict
                FileMetaEntity conflict = fileMetaDao.getByParentIdAndName(connection, node.getParentId(), newName);
                if (conflict != null) {
                    throw new FileConflictException("File with the same name already exists.");
                }

                fileMetaDao.updateParentIdAndName(connection, id, node.getParentId(), newName);

                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    public void copyNode(long id, long targetParentId, String strategy) throws Exception {
        try (Connection connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 1. 获取源节点
                FileMetaEntity source = fileMetaDao.getById(connection, id);
                if (source == null) {
                    throw new MoveException("Source node not found: " + id);
                }

                // 2. 检查目标是否存在
                if (targetParentId != 0) {
                    FileMetaEntity targetParent = fileMetaDao.getById(connection, targetParentId);
                    if (targetParent == null) {
                        throw new MoveException("Target folder not found: " + targetParentId);
                    }
                    if (targetParent.getIsFolder() == 0) {
                        throw new MoveException("Target is not a folder: " + targetParentId);
                    }
                }

                String targetName = source.getName();

                // 3. 冲突检测与处理
                FileMetaEntity conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId,
                        source.getName());
                if (conflict != null) {
                    if ("RENAME".equalsIgnoreCase(strategy)) {
                        String baseName = source.getName();
                        String nameWithoutExt = baseName;
                        String ext = "";
                        int lastDot = baseName.lastIndexOf('.');
                        if (lastDot > 0) {
                            nameWithoutExt = baseName.substring(0, lastDot);
                            ext = baseName.substring(lastDot);
                        }

                        int i = 1;
                        while (conflict != null) {
                            targetName = nameWithoutExt + " (" + i + ")" + ext;
                            conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId, targetName);
                            i++;
                        }
                    } else if ("OVERWRITE".equalsIgnoreCase(strategy)) {
                        if (conflict.getId() == id) {
                            throw new MoveException("Cannot overwrite file with itself");
                        }
                        if (source.getIsFolder() == 1 || conflict.getIsFolder() == 1) {
                            throw new MoveException("Cannot overwrite folder or with folder");
                        }

                        // Optimized: Direct update for file-to-file
                        if (!source.getHash().equals(conflict.getHash())) {
                            fileStorageDao.decrementReferenceCount(connection, conflict.getHash());
                            fileStorageDao.incrementReferenceCount(connection, source.getHash());
                        }

                        fileMetaDao.updateById(connection, conflict.getId(), source.getHash(), source.getSize(),
                                System.currentTimeMillis());
                        connection.commit();
                        return; // Done
                    } else {
                        throw new FileConflictException("File with the same name already exists.");
                    }
                }

                // 4. 执行递归复制
                copyNodeRecursively(connection, id, targetParentId, targetName);

                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private void copyNodeRecursively(Connection connection, long sourceId, long targetParentId, String targetName)
            throws SQLException {
        FileMetaEntity source = fileMetaDao.getById(connection, sourceId);
        if (source == null)
            return;

        if (source.getIsFolder() == 0) {
            // 文件：增加引用计数，插入新记录
            fileStorageDao.incrementReferenceCount(connection, source.getHash());
            fileMetaDao.insertFile(connection, targetParentId, targetName, source.getHash(), source.getSize(),
                    System.currentTimeMillis());
        } else {
            // 文件夹：创建新文件夹，递归复制子节点
            long newFolderId = fileMetaDao.insertFolder(connection, targetParentId, targetName,
                    System.currentTimeMillis());
            ArrayList<FileMetaEntity> children = fileMetaDao.getByParentId(connection, sourceId);
            for (FileMetaEntity child : children) {
                copyNodeRecursively(connection, child.getId(), newFolderId, child.getName());
            }
        }
    }
}

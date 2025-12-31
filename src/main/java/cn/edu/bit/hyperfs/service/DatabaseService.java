package cn.edu.bit.hyperfs.service;

import cn.edu.bit.hyperfs.db.*;
import cn.edu.bit.hyperfs.entity.FileMetaEntity;
import cn.edu.bit.hyperfs.entity.FileStorageEntity;

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
                        // 这里可以直接更新，或者先删除后插入。为了复用，这里选择删除旧的，插入新的（或者更新旧的）。
                        // 计划中说的是：减少原文件的引用计数。检查文件是否存在，不存在就插入，存在就增加引用计数。获取时间，创建节点，返回。
                        // 其实对于覆盖上传，应该更新元数据。

                        fileMetaDao.updateById(connection, existingNode.getId(), size, time);
                        // 还需要更新哈希值，但FileMetaDao.updateById目前只更新了size和time。
                        // 这是一个疏忽，需要修改FileMetaDao或者单独处理。
                        // 鉴于updateById目前实现，我们可以尝试删除旧节点，插入新节点（ID会变），或者修改update方法。
                        // 为了简单和一致性，并遵循"创建节点"的描述，我们删除旧节点，插入新节点。
                        fileMetaDao.removeById(connection, existingNode.getId());
                        long id = fileMetaDao.insertFile(connection, parentId, filename, hash, size, time);

                        connection.commit();
                        return new InsertFileResult(false, id);
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
}

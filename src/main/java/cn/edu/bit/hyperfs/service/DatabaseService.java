package cn.edu.bit.hyperfs.service;

import cn.edu.bit.hyperfs.db.*;
import cn.edu.bit.hyperfs.entity.FileMetaEntity;

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
     * 根据内容哈希值和大小查询文件内容是否已存在
     * 
     * 详细描述：
     * 该方法通过哈希值查找文件存储记录。如果找到了记录，
     * 还会进一步验证文件大小是否匹配，以防止哈希碰撞。
     *
     * @param hash 文件内容的哈希值
     * @param size 文件内容的大小
     * @return 如果存在且大小匹配返回true，否则返回false
     * @throws SQLException          数据库查询错误
     * @throws IllegalStateException 如果发现哈希碰撞（哈希值相同但大小不同）
     */
    public boolean existsByHash(String hash, long size) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            var entity = fileStorageDao.getByHash(connection, hash);
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
     * 列出指定文件夹下的所有文件和子文件夹
     * 
     * 详细描述：
     * 该方法查询数据库文件元数据表，返回指定父节点ID下的所有子节点列表。
     *
     * @param parentId 父节点ID
     * @return 包含文件元数据的列表
     * @throws SQLException 数据库查询错误
     */
    public ArrayList<FileMetaEntity> getList(long parentId) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            return fileMetaDao.getByParentId(connection, parentId);
        }
    }

    /**
     * 保存新上传的文件节点
     * 
     * 详细描述：
     * 该方法负责创建新的文件记录。它首先检查目标目录下是否存在同名文件。
     * 如果不存在同名文件，则在文件存储表中插入或更新引用计数，并在文件元数据表中创建新记录。
     * 如果存在同名文件，则根据哈希值判断是完全重复（直接返回）还是内容更新（更新元数据并调整引用计数）。
     *
     * @param parentId 父节点ID
     * @param filename 文件名
     * @param hash     文件内容的哈希值
     * @param size     文件大小
     * @return 插入结果对象，包含是否重复和节点ID
     * @throws Exception 业务逻辑异常或数据库异常
     */
    public InsertFileResult insertFile(long parentId, String filename, String hash, long size) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false); // 开启事务
            try {
                // 检查对应目录下有没有同名节点
                var existingNode = fileMetaDao.getByParentIdAndName(connection, parentId, filename);

                if (existingNode == null) {
                    // 没有同名节点
                    // 检查文件存储是否存在
                    var storage = fileStorageDao.getByHash(connection, hash);
                    if (storage == null) {
                        fileStorageDao.insertFile(connection, hash, size);
                    } else {
                        fileStorageDao.incrementReferenceCount(connection, hash);
                    }

                    var time = System.currentTimeMillis();
                    var id = fileMetaDao.insertFile(connection, parentId, filename, hash, size, time);
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
                        var storage = fileStorageDao.getByHash(connection, hash);
                        if (storage == null) {
                            fileStorageDao.insertFile(connection, hash, size);
                        } else {
                            fileStorageDao.incrementReferenceCount(connection, hash);
                        }

                        var time = System.currentTimeMillis();
                        fileMetaDao.updateById(connection, existingNode.getId(), hash, size, time);
                        connection.commit();
                        return new InsertFileResult(false, existingNode.getId());
                    }
                }
            } catch (Exception exception) {
                logger.error("Error inserting file, rolling back", exception);
                connection.rollback();
                throw exception;
            }
        }
    }

    /**
     * 撤销文件的插入操作
     * 
     * 详细描述：
     * 当上传过程后续步骤失败需要回滚时调用。它会删除文件元数据记录，
     * 并将对应的文件存储引用计数减一。
     *
     * @param result 之前的插入结果对象
     * @throws SQLException 数据库操作错误
     */
    public void revokeFileInsertion(InsertFileResult result) throws SQLException {
        if (result.isDuplicated()) {
            return;
        }
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 用ID获取节点信息
                var entity = fileMetaDao.getById(connection, result.id());
                if (entity != null) {
                    fileStorageDao.decrementReferenceCount(connection, entity.getHash());
                    fileMetaDao.removeById(connection, result.id());
                }
                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    /**
     * 创建新的文件夹节点
     * 
     * 详细描述：
     * 在指定父目录下创建一个新文件夹。如果存在同名节点，则抛出异常。
     *
     * @param parentId 父文件夹ID
     * @param filename 文件夹名称
     * @return 新创建的文件夹ID
     * @throws Exception 如果存在同名节点或其他数据库错误
     */
    public long insertFolder(long parentId, String filename) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                var existingNode = fileMetaDao.getByParentIdAndName(connection, parentId, filename);
                if (existingNode != null) {
                    throw new IllegalArgumentException("Node with same name already exists: " + filename);
                }
                var time = System.currentTimeMillis();
                var id = fileMetaDao.insertFolder(connection, parentId, filename, time);
                connection.commit();
                return id;
            } catch (Exception exception) {
                logger.error("Error inserting folder, rolling back", exception);
                connection.rollback();
                throw exception;
            }
        }
    }

    /**
     * 删除指定节点
     * 
     * 详细描述：
     * 递归删除指定ID的节点。如果是文件夹，会删除其所有子孙节点。
     * 如果是文件，会删除元数据并更新存储引用计数。
     *
     * @param id 要删除的节点ID
     * @throws SQLException 数据库错误
     */
    public void deleteNode(long id) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                deleteNodeRecursive(connection, id);
                connection.commit();
            } catch (Exception exception) {
                logger.error("Error deleting node, rolling back", exception);
                connection.rollback();
                throw exception;
            }
        }
    }

    private void deleteNodeRecursive(java.sql.Connection connection, long id) throws SQLException {
        var entity = fileMetaDao.getById(connection, id);
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
     * 增加文件的下载次数
     * 
     * 详细描述：
     * 更新指定文件节点的下载计数字段。
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
     * 获取文件节点的元数据信息
     * 
     * 详细描述：
     * 根据ID查询并返回节点的完整元数据。
     *
     * @param id 节点ID
     * @return 节点元数据实体，如果不存在则返回null
     * @throws SQLException 数据库错误
     */
    public FileMetaEntity getFileMeta(long id) throws SQLException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            return fileMetaDao.getById(connection, id);
        }
    }

    /**
     * 移动操作相关的通用异常
     */
    public static class MoveException extends Exception {
        public MoveException(String message) {
            super(message);
        }
    }

    /**
     * 文件名冲突异常
     */
    public static class FileConflictException extends MoveException {
        public FileConflictException(String message) {
            super(message);
        }
    }

    /**
     * 将节点移动到新的父节点下
     * 
     * 详细描述：
     * 实现文件或文件夹的移动功能。
     * 检查源节点与目标文件夹的合法性，防止将文件夹移动到自身的子目录中（环路检测）。
     * 根据提供的策略处理目标位置的文件名冲突：失败、重命名或覆盖。
     *
     * @param id             要移动的节点ID
     * @param targetParentId 目标父节点ID
     * @param strategy       冲突解决策略："FAIL"（失败）、"RENAME"（重命名）、"OVERWRITE"（覆盖）
     * @throws Exception 业务异常或数据库异常
     */
    public void moveNode(long id, long targetParentId, String strategy) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 检查源节点是否存在
                var source = fileMetaDao.getById(connection, id);
                if (source == null) {
                    throw new MoveException("Source node not found: " + id);
                }

                if (source.getParentId() == targetParentId) {
                    throw new MoveException("Source and destination folders are the same.");
                }

                // 检查目标文件夹是否存在（如果不是根目录）
                if (targetParentId != 0) {
                    var targetParent = fileMetaDao.getById(connection, targetParentId);
                    if (targetParent == null) {
                        throw new MoveException("Target folder not found: " + targetParentId);
                    }
                    if (targetParent.getIsFolder() == 0) {
                        throw new MoveException("Target is not a folder: " + targetParentId);
                    }
                }

                // 环路检测（如果源节点是文件夹）
                if (source.getIsFolder() == 1) {
                    var current = targetParentId;
                    while (current != 0) {
                        if (current == id) {
                            throw new MoveException("Cannot move a node into its own child");
                        }
                        var parent = fileMetaDao.getById(connection, current);
                        if (parent == null)
                            break;
                        current = parent.getParentId();
                    }
                }

                var finalName = source.getName();

                // 检查是否有名称冲突
                var conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId,
                        source.getName());
                if (conflict != null) {
                    // 根据策略处理冲突
                    if ("RENAME".equalsIgnoreCase(strategy)) {
                        // 寻找可用的名称：原名 (序号)
                        var baseName = source.getName();
                        var fileNameWithoutExtension = baseName;
                        var fileExtension = "";
                        var lastDotIndex = baseName.lastIndexOf('.');
                        if (lastDotIndex > 0) {
                            fileNameWithoutExtension = baseName.substring(0, lastDotIndex);
                            fileExtension = baseName.substring(lastDotIndex);
                        }

                        var renameSequence = 1;
                        while (conflict != null) {
                            finalName = fileNameWithoutExtension + " (" + renameSequence + ")" + fileExtension;
                            conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId, finalName);
                            renameSequence++;
                        }
                    } else if ("OVERWRITE".equalsIgnoreCase(strategy)) {
                        // 检查是否允许覆盖
                        if (source.getIsFolder() == 1 || conflict.getIsFolder() == 1) {
                            throw new MoveException("Cannot overwrite folder or with folder");
                        }
                        // 删除冲突节点
                        deleteNodeRecursive(connection, conflict.getId());
                    } else {
                        // 默认策略：失败
                        throw new FileConflictException(
                                "File with the same name already exists in the target directory.");
                    }
                }

                // 执行移动操作
                fileMetaDao.updateParentIdAndName(connection, id, targetParentId, finalName);

                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    /**
     * 重命名节点
     * 
     * 详细描述：
     * 如果新名称与旧名称相同，则直接返回。
     * 如果新名称在当前目录下已存在，则抛出冲突异常。
     * 否则，更新节点的名称字段。
     *
     * @param id      节点ID
     * @param newName 新名称
     * @throws SQLException  数据库错误
     * @throws MoveException 如果节点不存在或发生名称冲突
     */
    public void renameNode(long id, String newName) throws SQLException, MoveException {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                var node = fileMetaDao.getById(connection, id);
                if (node == null) {
                    throw new MoveException("Node not found");
                }

                if (node.getName().equals(newName)) {
                    return; // 名称未变更
                }

                // 检查名称冲突
                var conflict = fileMetaDao.getByParentIdAndName(connection, node.getParentId(), newName);
                if (conflict != null) {
                    throw new FileConflictException("File with the same name already exists.");
                }

                fileMetaDao.updateParentIdAndName(connection, id, node.getParentId(), newName);

                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    /**
     * 复制节点到新的位置
     * 
     * 详细描述：
     * 复制文件或文件夹到目标目录下。
     * 支持递归复制文件夹内容。
     * 处理文件名冲突，支持自动重命名或覆盖（仅限文件覆盖文件）。
     * 对于文件复制，采用引用计数增加的方式，而非物理复制。
     *
     * @param id             要复制的源节点ID
     * @param targetParentId 目标父节点ID
     * @param strategy       冲突解决策略："FAIL"、"RENAME"、"OVERWRITE"
     * @throws Exception 操作过程中发生的异常
     */
    public void copyNode(long id, long targetParentId, String strategy) throws Exception {
        try (var connection = DatabaseFactory.getInstance().getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                // 获取源节点
                var source = fileMetaDao.getById(connection, id);
                if (source == null) {
                    throw new MoveException("Source node not found: " + id);
                }

                // 检查目标文件夹是否存在
                if (targetParentId != 0) {
                    var targetParent = fileMetaDao.getById(connection, targetParentId);
                    if (targetParent == null) {
                        throw new MoveException("Target folder not found: " + targetParentId);
                    }
                    if (targetParent.getIsFolder() == 0) {
                        throw new MoveException("Target is not a folder: " + targetParentId);
                    }
                }

                var targetName = source.getName();

                // 冲突检测与处理
                var conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId,
                        source.getName());
                if (conflict != null) {
                    if ("RENAME".equalsIgnoreCase(strategy)) {
                        var baseName = source.getName();
                        var fileNameWithoutExtension = baseName;
                        var fileExtension = "";
                        var lastDotIndex = baseName.lastIndexOf('.');
                        if (lastDotIndex > 0) {
                            fileNameWithoutExtension = baseName.substring(0, lastDotIndex);
                            fileExtension = baseName.substring(lastDotIndex);
                        }

                        var renameSequence = 1;
                        while (conflict != null) {
                            targetName = fileNameWithoutExtension + " (" + renameSequence + ")" + fileExtension;
                            conflict = fileMetaDao.getByParentIdAndName(connection, targetParentId, targetName);
                            renameSequence++;
                        }
                    } else if ("OVERWRITE".equalsIgnoreCase(strategy)) {
                        if (conflict.getId() == id) {
                            throw new MoveException("Cannot overwrite file with itself");
                        }
                        if (source.getIsFolder() == 1 || conflict.getIsFolder() == 1) {
                            throw new MoveException("Cannot overwrite folder or with folder");
                        }

                        // 优化：针对文件覆盖文件的情况，直接更新，避免删除重建
                        if (!source.getHash().equals(conflict.getHash())) {
                            fileStorageDao.decrementReferenceCount(connection, conflict.getHash());
                            fileStorageDao.incrementReferenceCount(connection, source.getHash());
                        }

                        fileMetaDao.updateById(connection, conflict.getId(), source.getHash(), source.getSize(),
                                System.currentTimeMillis());
                        connection.commit();
                        return; // 完成覆盖操作
                    } else {
                        throw new FileConflictException("File with the same name already exists.");
                    }
                }

                // 执行递归复制
                copyNodeRecursively(connection, id, targetParentId, targetName);

                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    private void copyNodeRecursively(Connection connection, long sourceId, long targetParentId, String targetName)
            throws SQLException {
        var source = fileMetaDao.getById(connection, sourceId);
        if (source == null)
            return;

        if (source.getIsFolder() == 0) {
            // 文件：增加引用计数，插入新记录
            fileStorageDao.incrementReferenceCount(connection, source.getHash());
            fileMetaDao.insertFile(connection, targetParentId, targetName, source.getHash(), source.getSize(),
                    System.currentTimeMillis());
        } else {
            // 文件夹：创建新文件夹，递归复制子节点
            var newFolderId = fileMetaDao.insertFolder(connection, targetParentId, targetName,
                    System.currentTimeMillis());
            var children = fileMetaDao.getByParentId(connection, sourceId);
            for (var child : children) {
                copyNodeRecursively(connection, child.getId(), newFolderId, child.getName());
            }
        }
    }
}

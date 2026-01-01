package cn.edu.bit.hyperfs.db;

import cn.edu.bit.hyperfs.entity.FileMetaEntity;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class FileMetaDao {

    /**
     * 插入文件记录
     *
     * @param connection 数据库连接
     * @param parentId   父节点ID
     * @param filename   文件名
     * @param hash       文件哈希
     * @param size       文件大小
     * @param time       时间戳
     * @return 新插入的记录ID
     * @throws SQLException SQL异常
     */
    public long insertFile(Connection connection, long parentId, String filename, String hash, long size, long time)
            throws SQLException {
        var sql = "INSERT INTO file_meta (parent_id, name, is_folder, hash, sz, up_tm) VALUES (?, ?, 0, ?, ?, ?)";
        try (var preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setLong(1, parentId);
            preparedStatement.setString(2, filename);
            preparedStatement.setString(3, hash);
            preparedStatement.setLong(4, size);
            preparedStatement.setLong(5, time);
            preparedStatement.executeUpdate();

            try (var generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Failed to insert file, ID not obtained.");
                }
            }
        }
    }

    /**
     * 插入文件夹记录
     *
     * @param connection 数据库连接
     * @param parentId   父节点ID
     * @param filename   文件夹名
     * @param time       时间戳
     * @return 新插入的记录ID
     * @throws SQLException SQL异常
     */
    public long insertFolder(Connection connection, long parentId, String filename, long time) throws SQLException {
        var sql = "INSERT INTO file_meta (parent_id, name, is_folder, up_tm) VALUES (?, ?, 1, ?)";
        try (var preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setLong(1, parentId);
            preparedStatement.setString(2, filename);
            preparedStatement.setLong(3, time);
            preparedStatement.executeUpdate();

            try (var generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Failed to insert folder, ID not obtained.");
                }
            }
        }
    }

    /**
     * 根据ID删除节点
     *
     * @param connection 数据库连接
     * @param id         节点ID
     * @throws SQLException SQL异常
     */
    public void removeById(Connection connection, long id) throws SQLException {
        var sql = "DELETE FROM file_meta WHERE id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * 根据父节点ID删除所有子节点
     *
     * @param connection 数据库连接
     * @param parentId   父节点ID
     * @throws SQLException SQL异常
     */
    public void removeByParentId(Connection connection, long parentId) throws SQLException {
        var sql = "DELETE FROM file_meta WHERE parent_id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, parentId);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * 根据父节点ID获取该目录下的所有项
     *
     * @param connection 数据库连接
     * @param parentId   父节点ID
     * @return 文件元数据列表
     * @throws SQLException SQL异常
     */
    public ArrayList<FileMetaEntity> getByParentId(Connection connection, long parentId) throws SQLException {
        var list = new ArrayList<FileMetaEntity>();
        var sql = "SELECT id, parent_id, name, is_folder, hash, sz, up_tm, down_cnt FROM file_meta WHERE parent_id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, parentId);
            try (var resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    var entity = new FileMetaEntity();
                    entity.setId(resultSet.getLong("id"));
                    entity.setParentId(resultSet.getLong("parent_id"));
                    entity.setName(resultSet.getString("name"));
                    entity.setIsFolder(resultSet.getInt("is_folder"));
                    entity.setHash(resultSet.getString("hash"));
                    entity.setSize(resultSet.getLong("sz"));
                    entity.setUploadTime(resultSet.getLong("up_tm"));
                    entity.setDownloadCount(resultSet.getInt("down_cnt"));
                    list.add(entity);
                }
            }
        }
        return list;
    }

    /**
     * 根据ID更新文件元数据
     *
     * @param connection 数据库连接
     * @param id         节点ID
     * @param hash       新哈希
     * @param size       新大小
     * @param time       新时间戳
     * @throws SQLException SQL异常
     */
    public void updateById(Connection connection, long id, String hash, long size, long time) throws SQLException {
        var sql = "UPDATE file_meta SET hash = ?, sz = ?, up_tm = ? WHERE id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hash);
            preparedStatement.setLong(2, size);
            preparedStatement.setLong(3, time);
            preparedStatement.setLong(4, id);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * 增加节点下载次数
     *
     * @param connection 数据库连接
     * @param id         节点ID
     * @throws SQLException SQL异常
     */
    public void incrementDownloadCountById(Connection connection, long id) throws SQLException {
        var sql = "UPDATE file_meta SET down_cnt = down_cnt + 1 WHERE id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * 根据ID获取节点
     *
     * @param connection 数据库连接
     * @param id         节点ID
     * @return FileMetaEntity 或 null
     * @throws SQLException SQL异常
     */
    public FileMetaEntity getById(Connection connection, long id) throws SQLException {
        var sql = "SELECT id, parent_id, name, is_folder, hash, sz, up_tm, down_cnt FROM file_meta WHERE id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            try (var resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    var entity = new FileMetaEntity();
                    entity.setId(resultSet.getLong("id"));
                    entity.setParentId(resultSet.getLong("parent_id"));
                    entity.setName(resultSet.getString("name"));
                    entity.setIsFolder(resultSet.getInt("is_folder"));
                    entity.setHash(resultSet.getString("hash"));
                    entity.setSize(resultSet.getLong("sz"));
                    entity.setUploadTime(resultSet.getLong("up_tm"));
                    entity.setDownloadCount(resultSet.getInt("down_cnt"));
                    return entity;
                }
            }
        }
        return null;
    }

    /**
     * 检查同一目录下是否存在同名节点
     * 
     * @param connection 数据库连接
     * @param parentId   父节点ID
     * @param name       名称
     * @return FileMetaEntity 或 null
     * @throws SQLException
     */
    public FileMetaEntity getByParentIdAndName(Connection connection, long parentId, String name) throws SQLException {
        var sql = "SELECT id, parent_id, name, is_folder, hash, sz, up_tm, down_cnt FROM file_meta WHERE parent_id = ? AND name = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, parentId);
            preparedStatement.setString(2, name);
            try (var resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    var entity = new FileMetaEntity();
                    entity.setId(resultSet.getLong("id"));
                    entity.setParentId(resultSet.getLong("parent_id"));
                    entity.setName(resultSet.getString("name"));
                    entity.setIsFolder(resultSet.getInt("is_folder"));
                    entity.setHash(resultSet.getString("hash"));
                    entity.setSize(resultSet.getLong("sz"));
                    entity.setUploadTime(resultSet.getLong("up_tm"));
                    entity.setDownloadCount(resultSet.getInt("down_cnt"));
                    return entity;
                }
            }
        }
        return null;
    }

    /**
     * 更新节点的父ID和名称 (移动/重命名)
     *
     * @param connection 数据库连接
     * @param id         节点ID
     * @param parentId   新父ID
     * @param name       新名称
     * @throws SQLException SQL异常
     */
    public void updateParentIdAndName(Connection connection, long id, long parentId, String name) throws SQLException {
        var sql = "UPDATE file_meta SET parent_id = ?, name = ? WHERE id = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, parentId);
            preparedStatement.setString(2, name);
            preparedStatement.setLong(3, id);
            preparedStatement.executeUpdate();
        }
    }
}

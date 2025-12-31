package cn.edu.bit.hyperfs.db;

import cn.edu.bit.hyperfs.entity.FileStorageEntity;

import java.sql.Connection;
import java.sql.SQLException;

public class FileStorageDao {

    /**
     * 插入文件记录
     * 如果已存在则抛出异常
     *
     * @param connection 数据库连接
     * @param hash       文件哈希
     * @param size       文件大小
     * @throws SQLException SQL异常
     */
    public void insertFile(Connection connection, String hash, long size) throws SQLException {
        var sql = "INSERT INTO file_storage (hash, sz) VALUES (?, ?)";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hash);
            preparedStatement.setLong(2, size);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * 增加文件引用计数
     *
     * @param connection 数据库连接
     * @param hash       文件哈希
     * @throws SQLException 如果影响行数不为1
     */
    public void incrementReferenceCount(Connection connection, String hash) throws SQLException {
        var sql = "UPDATE file_storage SET ref_cnt = ref_cnt + 1 WHERE hash = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hash);
            int affectedRows = preparedStatement.executeUpdate();
            if (affectedRows != 1) {
                throw new SQLException(
                        "Failed to increment reference count, record not found or multiple records modified: " + hash);
            }
        }
    }

    /**
     * 减少文件引用计数
     *
     * @param connection 数据库连接
     * @param hash       文件哈希
     * @throws SQLException 如果影响行数不为1
     */
    public void decrementReferenceCount(Connection connection, String hash) throws SQLException {
        var sql = "UPDATE file_storage SET ref_cnt = ref_cnt - 1 WHERE hash = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hash);
            int affectedRows = preparedStatement.executeUpdate();
            if (affectedRows != 1) {
                throw new SQLException(
                        "Failed to decrement reference count, record not found or multiple records modified: " + hash);
            }
        }
    }

    /**
     * 根据哈希值获取文件存储记录
     *
     * @param connection 数据库连接
     * @param hash       文件哈希
     * @return FileStorageEntity 或 null
     * @throws SQLException SQL异常
     */
    public FileStorageEntity getByHash(Connection connection, String hash) throws SQLException {
        var sql = "SELECT hash, sz, ref_cnt, created_at FROM file_storage WHERE hash = ?";
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hash);
            try (var resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    var entity = new FileStorageEntity();
                    entity.setHash(resultSet.getString("hash"));
                    entity.setSize(resultSet.getLong("sz"));
                    entity.setReferenceCount(resultSet.getInt("ref_cnt"));
                    entity.setCreatedAt(resultSet.getString("created_at"));
                    return entity;
                }
            }
        }
        return null;
    }
}

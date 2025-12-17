package cn.edu.bit.hyperfs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import cn.edu.bit.hyperfs.entity.FileStorageData;

public class FileStorageDao {
	private final DataSource dataSource = DatabaseFactory.getInstance().getDataSource();

	private FileStorageData getByHashValue(Connection connection, String hashValue) {
		String sql = "SELECT sz, ref_cnt FROM file_storage WHERE hash = ?";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, hashValue);
			var resultSet = statement.executeQuery();
			if (resultSet.next()) {
				long fileSize = resultSet.getLong("sz");
				int referenceCount = resultSet.getInt("ref_cnt");
				return new FileStorageData(hashValue, fileSize, referenceCount);
			}
			return null;
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to query FileStorageData by hash value", exception);
		}
	}

	public FileStorageData getByHashValue(String hashValue) {
		try (var connection = dataSource.getConnection()) {
			return getByHashValue(connection, hashValue);
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to get FileStorageData by hash value", exception);
		}
	}

	private void incrementReferenceCount(Connection connection, String hashValue) {
		String sql = "UPDATE file_storage SET ref_cnt = ref_cnt + 1 WHERE hash = ?";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, hashValue);
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to increment reference count", exception);
		}
	}

	private void insertNewRecord(Connection connection, String hashValue, long fileSize) {
		String sql = "INSERT INTO file_storage (hash, sz, ref_cnt) VALUES (?, ?, 1)";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, hashValue);
			statement.setLong(2, fileSize);
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to insert new FileStorageData record", exception);
		}
	}

	/**
	 * 检查哈希值是否存在，如果不存在就插入新纪录，如果存在就检查大小是否匹配，如果不匹配会抛出异常，匹配则增加引用计数并返回新的引用计数
	 * 
	 * @param hashValue
	 * @param fileSize
	 * @return 引用计数
	 */
	public int insertOrIncrement(String hashValue, long fileSize) {
		if (hashValue.length() != 64) {
			throw new IllegalArgumentException("Hash value must be 64 characters long");
		}

		try (var connection = dataSource.getConnection()) {
			// 检查是否存在
			var existingData = getByHashValue(connection, hashValue);
			if (existingData != null) {
				long existingSize = existingData.getFileSize();
				if (existingSize != fileSize) {
					throw new IllegalArgumentException(
							"File size mismatch for existing hash value, probably a hash collision occurred");
				}
				// 增加引用计数
				incrementReferenceCount(connection, hashValue);
				return existingData.getReferenceCount() + 1;
			}

			// 不存在则插入新纪录
			insertNewRecord(connection, hashValue, fileSize);
			return 1;
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to insert or increment FileStorageData", exception);
		}
	}

	private void decrementReferenceCount(Connection connection, String hashValue) {
		String sql = "UPDATE file_storage SET ref_cnt = ref_cnt - 1 WHERE hash = ?";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, hashValue);
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to decrement reference count", exception);
		}
	}

	private void deleteRecord(Connection connection, String hashValue) {
		String sql = "DELETE FROM file_storage WHERE hash = ?";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, hashValue);
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to delete FileStorageData record", exception);
		}
	}

	/**
	 * 减少引用计数，返回新的引用计数；如果引用计数被减少到0，则删除该记录
	 * @param hashValue
	 * @return
	 */
	public int decrementReferenceCount(String hashValue) {
		try (var connection = dataSource.getConnection()) {
			var existingData = getByHashValue(connection, hashValue);
			if (existingData == null) {
				throw new IllegalArgumentException("No such hash value exists in the database");
			}

			int currentRefCount = existingData.getReferenceCount();
			if (currentRefCount <= 0) {
				throw new IllegalStateException("Reference count is already zero or negative");
			}
			decrementReferenceCount(connection, hashValue);

			int newRefCount = currentRefCount - 1;
			if (newRefCount == 0) {
				deleteRecord(connection, hashValue);
			}

			return newRefCount;
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to decrement reference count", exception);
		}
	}
}

package cn.edu.bit.hyperfs.db;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.sql.DataSource;

import cn.edu.bit.hyperfs.entity.FileMetaNode;

public class FileMetaDao {
	private final DataSource dataSource = DatabaseFactory.getInstance().getDataSource();

	public FileMetaNode getById(long id) {
		try (var connection = dataSource.getConnection()) {
			String sql = "SELECT parent_id, name, is_folder, hash, sz, up_tm, down_cnt FROM file_meta WHERE id = ?";
			try (var statement = connection.prepareStatement(sql)) {
				statement.setLong(1, id);
				var resultSet = statement.executeQuery();
				if (resultSet.next()) {
					long parentId = resultSet.getLong("parent_id");
					String name = resultSet.getString("name");
					boolean isFolder = resultSet.getInt("is_folder") != 0;
					String hashValue = resultSet.getString("hash");
					long size = resultSet.getLong("sz");
					long uploadTime = resultSet.getLong("up_tm");
					int downloadCount = resultSet.getInt("down_cnt");
					return new FileMetaNode(id, parentId, name, isFolder, hashValue, size, uploadTime, downloadCount);
				}
				return null;
			}
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to get FileMetaNode by ID", exception);
		}
	}

	public ArrayList<FileMetaNode> getByParentId(long parentId) {
		try (var connection = dataSource.getConnection()) {
			String sql = "SELECT id, name, is_folder, hash, sz, up_tm, down_cnt FROM file_meta WHERE parent_id = ?";
			try (var statement = connection.prepareStatement(sql)) {
				statement.setLong(1, parentId);
				var resultSet = statement.executeQuery();
				var nodeList = new ArrayList<FileMetaNode>();
				while (resultSet.next()) {
					long id = resultSet.getLong("id");
					String name = resultSet.getString("name");
					boolean isFolder = resultSet.getBoolean("is_folder");
					String hashValue = resultSet.getString("hash");
					long size = resultSet.getLong("sz");
					long uploadTime = resultSet.getLong("up_tm");
					int downloadCount = resultSet.getInt("down_cnt");
					var node = new FileMetaNode(id, parentId, name, isFolder, hashValue, size, uploadTime,
							downloadCount);
					nodeList.add(node);
				}
				return nodeList;
			}
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to get FileMetaNodes by parent ID", exception);
		}
	}

	/**
	 * 插入文件元数据节点，修改node的ID字段并返回新插入节点的ID
	 * 
	 * @param node
	 * @return 新插入节点的ID
	 */
	public long insertFileMetaNode(FileMetaNode node) {
		String sql = "INSERT INTO file_meta (parent_id, name, is_folder, hash, sz, up_tm, down_cnt) VALUES (?, ?, ?, ?, ?, ?, ?)";
		try (var connection = dataSource.getConnection();
				var statement = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {
			statement.setLong(1, node.getParentId());
			statement.setString(2, node.getName());
			statement.setBoolean(3, node.getNodeType());
			statement.setString(4, node.getHashValue());
			statement.setLong(5, node.getSize());
			statement.setLong(6, node.getUploadTime());
			statement.setInt(7, node.getDownloadCount());
			statement.executeUpdate();

			var resultSet = statement.getGeneratedKeys();
			if (resultSet.next()) {
				node.setId(resultSet.getLong(1));
				return node.getId();
			} else {
				throw new DataAccessException("Failed to retrieve generated ID for inserted FileMetaNode");
			}
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to insert FileMetaNode", exception);
		}
	}

	/**
	 * 按ID更新文件元数据节点
	 * 
	 * @param node
	 */
	public void updateFileMetaNode(FileMetaNode node) {
		String sql = "UPDATE file_meta SET parent_id = ?, name = ?, is_folder = ?, hash = ?, sz = ?, up_tm = ?, down_cnt = ? WHERE id = ?";
		try (var connection = dataSource.getConnection();
				var statement = connection.prepareStatement(sql)) {
			statement.setLong(1, node.getParentId());
			statement.setString(2, node.getName());
			statement.setBoolean(3, node.getNodeType());
			statement.setString(4, node.getHashValue());
			statement.setLong(5, node.getSize());
			statement.setLong(6, node.getUploadTime());
			statement.setInt(7, node.getDownloadCount());
			statement.setLong(8, node.getId());
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to update FileMetaNode", exception);
		}
	}

	public void incrementDownloadCount(long id) {
		String sql = "UPDATE file_meta SET down_cnt = down_cnt + 1 WHERE id = ?";
		try (var connection = dataSource.getConnection();
				var statement = connection.prepareStatement(sql)) {
			statement.setLong(1, id);
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to increment download count", exception);
		}
	}

	public void deleteById(long id) {
		String sql = "DELETE FROM file_meta WHERE id = ?";
		try (var connection = dataSource.getConnection();
				var statement = connection.prepareStatement(sql)) {
			statement.setLong(1, id);
			statement.executeUpdate();
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to delete FileMetaNode by ID", exception);
		}
	}
}

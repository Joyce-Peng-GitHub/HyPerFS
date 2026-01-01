package cn.edu.bit.hyperfs.db;

import java.io.File;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseFactory {
	private static final Logger logger = LoggerFactory.getLogger(DatabaseFactory.class);
	private static volatile DatabaseFactory instance;
	private final HikariDataSource dataSource;

	public static final String DATABASE_PATH = "./db/hyperfs.db";

	private DatabaseFactory() {
		logger.info("Initializing DatabaseFactory...");
		ensureDatabaseDirectoryExists();

		var hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl("jdbc:sqlite:" + DATABASE_PATH);
		hikariConfig.setPoolName("HyPerFSPool");
		hikariConfig.setMaximumPoolSize(10);
		hikariConfig.setConnectionInitSql("PRAGMA journal_mode = WAL;");

		dataSource = new HikariDataSource(hikariConfig);

		initTables();
		logger.info("Database initialized successfully at {}", DATABASE_PATH);
	}

	/**
	 * 获取数据库工厂单例
	 * 
	 * 详细描述：
	 * 使用双重检查锁定模式（Double-Checked Locking）实现线程安全的单例获取。
	 *
	 * @return DatabaseFactory 实例
	 */
	public static DatabaseFactory getInstance() {
		if (instance == null) {
			synchronized (DatabaseFactory.class) {
				if (instance == null) {
					instance = new DatabaseFactory();
				}
			}
		}
		return instance;
	}

	/**
	 * 获取数据源
	 * 
	 * 详细描述：
	 * 返回配置好的 HikariDataSource 连接池实例。
	 *
	 * @return HikariDataSource 数据源
	 */
	public HikariDataSource getDataSource() {
		return dataSource;
	}

	private void ensureDatabaseDirectoryExists() {
		var databaseFile = new File(DATABASE_PATH);
		var parentDirectory = databaseFile.getParentFile();
		if (parentDirectory != null && !parentDirectory.exists()) {
			parentDirectory.mkdirs();
		}
	}

	private void initTables() {
		try (var connection = dataSource.getConnection();
				var statement = connection.createStatement()) {

			// 创建 file_storage 表
			statement.execute("""
					    CREATE TABLE IF NOT EXISTS file_storage (
					        hash TEXT NOT NULL PRIMARY KEY,
					        sz INTEGER NOT NULL,
					        ref_cnt INTEGER NOT NULL DEFAULT 1,
					        created_at TEXT DEFAULT CURRENT_TIMESTAMP
					    ) STRICT;
					""");

			// 创建 file_meta 表
			statement.execute("""
					    CREATE TABLE IF NOT EXISTS file_meta (
					        id INTEGER PRIMARY KEY,
					        parent_id INTEGER NOT NULL DEFAULT 0,
					        name TEXT NOT NULL,
					        is_folder INTEGER NOT NULL,
					        hash TEXT,
					        sz INTEGER DEFAULT 0,
					        up_tm INTEGER NOT NULL,
					        down_cnt INTEGER NOT NULL DEFAULT 0,
					        UNIQUE (parent_id, name)
					    ) STRICT;
					""");

			// 创建索引
			statement.execute("""
					    CREATE INDEX IF NOT EXISTS idx_parent_id
					    ON file_meta (parent_id);
					""");

		} catch (SQLException exception) {
			throw new RuntimeException("Failed to initialize database: " + exception.getMessage(), exception);
		}
	}
}

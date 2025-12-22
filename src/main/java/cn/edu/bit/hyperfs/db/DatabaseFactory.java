package cn.edu.bit.hyperfs.db;

import java.sql.Statement;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DatabaseFactory {
	private static volatile DatabaseFactory instance;
	private final HikariDataSource dataSource;

	public static final String DATABASE_PATH = "./db/hyperfs.db";

	private DatabaseFactory() {
		ensureDatabaseDirectoryExists();

		var config = new HikariConfig();
		config.setJdbcUrl("jdbc:sqlite:" + DATABASE_PATH);
		config.setPoolName("HyPerFSPool");
		config.setMaximumPoolSize(10);
		config.setConnectionInitSql("PRAGMA journal_mode = WAL;");

		dataSource = new HikariDataSource(config);

		initTables();
	}

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
		String createStorageTable = """
					CREATE TABLE IF NOT EXISTS file_storage (
						hash TEXT NOT NULL PRIMARY KEY,
						sz INTEGER NOT NULL,
						ref_cnt INTEGER NOT NULL DEFAULT 1,
						created_at TEXT DEFAULT CURRENT_TIMESTAMP
					) STRICT;
				""";
		String createFileMetaTable = """
					CREATE TABLE IF NOT EXISTS file_meta (
						id INTEGER PRIMARY KEY,
						parent_id INTEGER NOT NULL DEFAULT 0,
						name TEXT NOT NULL,
						is_folder INTEGER NOT NULL,
						hash TEXT,
						sz INTEGER NOT NULL DEFAULT 0,
						up_tm INTEGER NOT NULL,
						down_cnt INTEGER NOT NULL DEFAULT 0,
						UNIQUE (parent_id, name)
					) STRICT;
				""";
		String createIndexParentId = """
					CREATE INDEX IF NOT EXISTS idx_parent_id
					ON file_meta (parent_id);
				""";

		try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
			statement.execute(createStorageTable);
			statement.execute(createFileMetaTable);
			System.out.println("Tables initialized successfully.");

			statement.execute(createIndexParentId);
			System.out.println("Indexes initialized successfully.");
		} catch (SQLException exception) {
			throw new DataAccessException("Failed to initialize database tables", exception);
		}
	}
}

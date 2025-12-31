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
	}
}

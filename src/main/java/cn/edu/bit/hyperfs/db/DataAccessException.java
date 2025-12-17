package cn.edu.bit.hyperfs.db;

public class DataAccessException extends RuntimeException {
	public DataAccessException(String message, Throwable cause) {
		super(message, cause);
	}
}

package cn.edu.bit.hyperfs.service;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

import io.netty.buffer.ByteBuf;

public class FileUploadSession {
	public static class UploadResult {
		public final File file;
		public final byte[] hashValue;
		public final long fileSize;

		public UploadResult(File file, byte[] hashValue, long fileSize) {
			this.file = file;
			this.hashValue = hashValue;
			this.fileSize = fileSize;
		}
	}

	private final File tmpFile; // 临时文件句柄
	private final FileChannel fileChannel; // NIO 文件通道 (高性能写入)
	private final MessageDigest digest; // 哈希计算
	private long receivedBytes = 0;

	public FileUploadSession(File tmpDir) throws Exception {
		this.tmpFile = File.createTempFile("upload_", ".tmp", tmpDir);
		this.fileChannel = FileChannel.open(tmpFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		this.digest = MessageDigest.getInstance("SHA-256");
	}

	/**
	 * 处理上传的文件块，更新哈希值并写入临时文件
	 * 
	 * @param chunk
	 * @throws Exception
	 */
	public void processChunk(ByteBuf chunk) throws Exception {
		var length = chunk.readableBytes();

		// 更新哈希值
		chunk.markReaderIndex();
		digest.update(chunk.nioBuffer());
		chunk.resetReaderIndex();

		// 写入临时文件
		fileChannel.write(chunk.nioBuffer());
		receivedBytes += length;
	}

	/**
	 * 完成上传，关闭文件通道并返回上传结果
	 * 
	 * @return UploadResult 包含临时文件、哈希值和文件大小
	 * @throws Exception
	 */
	public UploadResult finish() throws Exception {
		fileChannel.force(true);
		fileChannel.close();
		byte[] hashValue = digest.digest();
		return new UploadResult(tmpFile, hashValue, receivedBytes);
	}

	/**
	 * 中止上传，关闭文件通道并删除临时文件
	 * 
	 * @throws Exception
	 */
	public void abort() throws Exception {
		fileChannel.close();
		if (tmpFile.exists()) {
			tmpFile.delete();
		}
	}
}

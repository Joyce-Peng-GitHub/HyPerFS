package cn.edu.bit.hyperfs.service;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;

import io.netty.buffer.ByteBuf;

public class FileUploadSession {
	private final File temporaryFile; // 临时文件句柄
	private final FileChannel fileChannel; // NIO 文件通道
	private final MessageDigest messageDigest; // 哈希计算
	private long receivedBytes = 0;

	public FileUploadSession(File tmpDirectory) throws Exception {
		this.temporaryFile = File.createTempFile("upload_", ".tmp", tmpDirectory);
		this.fileChannel = FileChannel.open(temporaryFile.toPath(), StandardOpenOption.CREATE,
				StandardOpenOption.WRITE);
		this.messageDigest = MessageDigest.getInstance("SHA-256");
	}

	public void processChunk(ByteBuf chunk) throws Exception {
		int readableBytes = chunk.readableBytes();
		if (readableBytes > 0) {
			// 更新哈希
			chunk.markReaderIndex();
			byte[] bytes = new byte[readableBytes];
			chunk.readBytes(bytes);
			messageDigest.update(bytes);
			chunk.resetReaderIndex();

			// 写入文件
			receivedBytes += fileChannel.write(chunk.nioBuffer());
		}
	}

	public void rename(File dest) throws IOException {
		close(); // 确保先关闭通道
		if (dest.exists()) {
			dest.delete();
		}
		if (!temporaryFile.renameTo(dest)) {
			throw new IOException("Failed to rename temporary file: " + temporaryFile.getAbsolutePath() + " -> "
					+ dest.getAbsolutePath());
		}
	}

	public void delete() throws IOException {
		close();
		if (temporaryFile.exists()) {
			temporaryFile.delete();
		}
	}

	public String getHashString() {
		return HexFormat.of().formatHex(messageDigest.digest());
	}

	public long getSize() {
		return receivedBytes;
	}

	public void close() throws IOException {
		if (fileChannel.isOpen()) {
			fileChannel.close();
		}
	}

	public void abort() {
		try {
			delete();
		} catch (IOException e) {
			// 忽略异常
		}
	}
}

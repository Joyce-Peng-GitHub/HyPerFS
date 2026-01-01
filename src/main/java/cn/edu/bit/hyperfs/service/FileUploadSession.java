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

	/**
	 * 构造函数：创建上传会话
	 *
	 * @param temporaryDirectory 临时目录
	 * @throws Exception 如果创建临时文件失败
	 */
	public FileUploadSession(File temporaryDirectory) throws Exception {
		this.temporaryFile = File.createTempFile("upload_", ".tmp", temporaryDirectory);
		this.fileChannel = FileChannel.open(temporaryFile.toPath(), StandardOpenOption.CREATE,
				StandardOpenOption.WRITE);
		this.messageDigest = MessageDigest.getInstance("SHA-256");
	}

	/**
	 * 处理数据块
	 * 
	 * 详细描述：
	 * 读取数据块，更新哈希计算，并写入临时文件。
	 *
	 * @param chunk 数据块
	 * @throws Exception 写入失败
	 */
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

	/**
	 * 重命名（移动）临时文件到目标位置
	 *
	 * @param destination 目标文件
	 * @throws IOException IO异常
	 */
	public void rename(File destination) throws IOException {
		close(); // 确保先关闭通道
		if (destination.exists()) {
			destination.delete();
		}
		if (!temporaryFile.renameTo(destination)) {
			throw new IOException("Failed to rename temporary file: " + temporaryFile.getAbsolutePath() + " -> "
					+ destination.getAbsolutePath());
		}
	}

	/**
	 * 删除临时文件
	 *
	 * @throws IOException 删除失败
	 */
	public void delete() throws IOException {
		close();
		if (temporaryFile.exists()) {
			temporaryFile.delete();
		}
	}

	/**
	 * 获取文件的SHA-256哈希值字符串
	 *
	 * @return 16进制哈希字符串
	 */
	public String getHashString() {
		return HexFormat.of().formatHex(messageDigest.digest());
	}

	/**
	 * 获取已接收的字节数
	 *
	 * @return 文件大小
	 */
	public long getSize() {
		return receivedBytes;
	}

	/**
	 * 关闭资源
	 *
	 * @throws IOException 关闭失败
	 */
	public void close() throws IOException {
		if (fileChannel.isOpen()) {
			fileChannel.close();
		}
	}

	/**
	 * 中止上传（尝试删除临时文件）
	 */
	public void abort() {
		try {
			delete();
		} catch (IOException exception) {
			// 忽略异常
		}
	}
}

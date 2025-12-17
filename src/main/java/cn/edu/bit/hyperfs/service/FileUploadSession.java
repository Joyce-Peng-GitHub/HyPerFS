package cn.edu.bit.hyperfs.service;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

import cn.edu.bit.hyperfs.entity.FileStorageData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class FileUploadSession {
	private final File tmpFile; // 临时文件句柄
	private final FileChannel fileChannel; // NIO 文件通道 (高性能写入)
	private final MessageDigest digest; // 哈希计算
	private long receivedBytes = 0;

	public FileUploadSession(File tmpDirectory) throws Exception {
		this.tmpFile = File.createTempFile("upload_", ".tmp", tmpDirectory);
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
		
		chunk.markReaderIndex();
		var buffer = new byte[length];
		chunk.readBytes(buffer);
		System.out.println(new String(buffer));
		chunk.resetReaderIndex();

		// 写入临时文件
		fileChannel.write(chunk.nioBuffer());
		receivedBytes += length;
	}

	/**
	 * 完成文件写入，关闭文件通道，将临时文件移动到数据目录，返回写入结果
	 * 
	 * @return FileStorageData
	 * @throws Exception
	 */
	public FileStorageData finish(File dataDirectory) throws Exception {
		fileChannel.force(true);
		fileChannel.close();

		byte[] hashValue = digest.digest();

		// TODO: 检查文件是否已存在，若存在则删除临时文件并增加引用计数

		// 将临时文件移动到数据目录
		var finalFile = new File(dataDirectory, ByteBufUtil.hexDump(hashValue));
		if (!tmpFile.renameTo(finalFile)) {
			throw new Exception("Failed to move temporary file to data directory");
		}

		return new FileStorageData(hashValue, receivedBytes, 1);
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

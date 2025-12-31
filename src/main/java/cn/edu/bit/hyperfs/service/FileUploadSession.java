package cn.edu.bit.hyperfs.service;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class FileUploadSession {
	private final File temporaryFile; // 临时文件句柄
	private final FileChannel fileChannel; // NIO 文件通道
	private final MessageDigest messageDigest; // 哈希计算
	private long receivedBytes = 0;

	public FileUploadSession(File tmpDirectory) throws Exception {
		this.temporaryFile = File.createTempFile("upload_", ".tmp", tmpDirectory);
		this.fileChannel = FileChannel.open(temporaryFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		this.messageDigest = MessageDigest.getInstance("SHA-256");
	}

	public void processChunk(ByteBuf chunk) throws Exception {
	}
}

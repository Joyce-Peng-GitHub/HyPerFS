package cn.edu.bit.hyperfs.entity;

public class FileMetadata {
    private byte[] hashValue; // 文件哈希值
    private long fileSize; // 文件大小
    private int referenceCount; // 引用计数

    public FileMetadata(byte[] hashValue, long fileSize, int referenceCount) {
        setHashValue(hashValue);
        setFileSize(fileSize);
        setReferenceCount(referenceCount);
    }

    public FileMetadata() {
        this(null, 0, 0);
    }

    public byte[] getHashValue() {
        return hashValue;
    }

    public void setHashValue(byte[] hashValue) {
        if (hashValue.length != 32) {
            throw new IllegalArgumentException("Hash value must be 32 bytes long");
        }
        this.hashValue = hashValue;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        if (fileSize < 0) {
            throw new IllegalArgumentException("File size cannot be negative");
        }
        this.fileSize = fileSize;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public void setReferenceCount(int referenceCount) {
        if (referenceCount < 0) {
            throw new IllegalArgumentException("Reference count cannot be negative");
        }
        this.referenceCount = referenceCount;
    }
}

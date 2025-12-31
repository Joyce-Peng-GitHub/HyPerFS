package cn.edu.bit.hyperfs.entity;

/**
 * 文件存储实体类
 * 对应数据库表: file_storage
 */
public class FileStorageEntity {
    /**
     * 文件内容的哈希值
     * DB: hash
     */
    private String hash;

    /**
     * 文件大小
     * DB: sz
     */
    private long size;

    /**
     * 引用计数
     * DB: ref_cnt
     */
    private int referenceCount;

    /**
     * 创建时间
     * DB: created_at
     */
    private String createdAt;

    public FileStorageEntity() {
    }

    public FileStorageEntity(String hash, long size, int referenceCount, String createdAt) {
        this.hash = hash;
        this.size = size;
        this.referenceCount = referenceCount;
        this.createdAt = createdAt;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "FileStorageEntity{" +
                "hash='" + hash + '\'' +
                ", size=" + size +
                ", referenceCount=" + referenceCount +
                ", createdAt='" + createdAt + '\'' +
                '}';
    }
}

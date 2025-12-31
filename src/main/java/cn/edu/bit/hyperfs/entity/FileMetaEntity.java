package cn.edu.bit.hyperfs.entity;

/**
 * 文件元数据实体类
 * 对应数据库表: file_meta
 */
public class FileMetaEntity {
    /**
     * 节点ID
     * DB: id
     */
    private long id;

    /**
     * 父节点ID
     * DB: parent_id
     */
    private long parentId;

    /**
     * 文件名或文件夹名
     * DB: name
     */
    private String name;

    /**
     * 是否为文件夹 (1: 是, 0: 否)
     * DB: is_folder
     */
    private int isFolder;

    /**
     * 文件哈希值 (如果是文件夹则为null)
     * DB: hash
     */
    private String hash;

    /**
     * 文件大小 (如果是文件夹则为0)
     * DB: sz
     */
    private long size;

    /**
     * 上传时间 (时间戳)
     * DB: up_tm
     */
    private long uploadTime;

    /**
     * 下载次数
     * DB: down_cnt
     */
    private int downloadCount;

    public FileMetaEntity() {
    }

    public FileMetaEntity(long id, long parentId, String name, int isFolder, String hash, long size, long uploadTime,
            int downloadCount) {
        this.id = id;
        this.parentId = parentId;
        this.name = name;
        this.isFolder = isFolder;
        this.hash = hash;
        this.size = size;
        this.uploadTime = uploadTime;
        this.downloadCount = downloadCount;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIsFolder() {
        return isFolder;
    }

    public void setIsFolder(int isFolder) {
        this.isFolder = isFolder;
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

    public long getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(long uploadTime) {
        this.uploadTime = uploadTime;
    }

    public int getDownloadCount() {
        return downloadCount;
    }

    public void setDownloadCount(int downloadCount) {
        this.downloadCount = downloadCount;
    }

    @Override
    public String toString() {
        return "FileMetaEntity{" +
                "id=" + id +
                ", parentId=" + parentId +
                ", name='" + name + '\'' +
                ", isFolder=" + isFolder +
                ", hash='" + hash + '\'' +
                ", size=" + size +
                ", uploadTime=" + uploadTime +
                ", downloadCount=" + downloadCount +
                '}';
    }
}

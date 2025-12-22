package cn.edu.bit.hyperfs.entity;

public class FileMetaNode {
    public enum NodeType {
        FILE,
        DIRECTORY
    }

    private long id; // 标识该条记录的唯一ID
    private long parentId; // 父节点ID，根节点为null
    private String name; // 文件或文件夹名称
    private boolean nodeType; // 是否为文件夹
    private String hashValue; // 文件哈希值，文件夹则为null
    private long size; // 文件大小（字节）
    private long uploadTime; // 文件的上传时间或文件夹的最后修改时间
    private int downloadCount; // 文件下载次数或文件夹中文件的总下载次数

    public FileMetaNode() {
        this(0, 0, null, false, null, 0, 0, 0);
    }

    public FileMetaNode(long id, long parentId, String name, boolean nodeType,
            String hashValue, long size, long uploadTime, int downloadCount) {
        setId(id);
        setParentId(parentId);
        setName(name);
        setNodeType(nodeType);
        setHashValue(hashValue);
        setSize(size);
        setUploadTime(uploadTime);
        setDownloadCount(downloadCount);
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

    public boolean getNodeType() {
        return nodeType;
    }

    public void setNodeType(boolean nodeType) {
        this.nodeType = nodeType;
    }

    public String getHashValue() {
        return hashValue;
    }

    public void setHashValue(String hashValue) {
        if (hashValue != null && hashValue.length() != 64) {
            throw new IllegalArgumentException("Hash value must be 64 characters long");
        }
        this.hashValue = hashValue;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative");
        }
        this.size = size;
    }

    public long getUploadTime() {
        return uploadTime;
    }

    public void setUploadTime(long uploadTime) {
        if (uploadTime < 0) {
            throw new IllegalArgumentException("Upload time cannot be negative");
        }
        this.uploadTime = uploadTime;
    }

    public int getDownloadCount() {
        return downloadCount;
    }

    public void setDownloadCount(int downloadCount) {
        if (downloadCount < 0) {
            throw new IllegalArgumentException("Download count cannot be negative");
        }
        this.downloadCount = downloadCount;
    }

    public void incrementDownloadCount() {
        ++this.downloadCount;
    }
}

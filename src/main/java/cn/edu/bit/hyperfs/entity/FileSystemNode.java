package cn.edu.bit.hyperfs.entity;

public class FileSystemNode {
    public enum NodeType {
        FILE,
        DIRECTORY
    }

    private long id; // 标识该条记录的唯一ID
    private long parentId; // 父节点ID，根节点为null
    private String name; // 文件或文件夹名称
    private NodeType nodeType; // 文件系统节点类型，文件或文件夹
    private FileStorageData fileStorageData; // 文件元数据，文件夹则为null
    private FileSystemNodeStatistics fileSystemNodeStatistics; // 文件统计信息，文件夹则为总和

    public FileSystemNode() {
        this(0, 0, null, null, null, null);
    }

    public FileSystemNode(long id, long parentId, String name, NodeType type,
            FileStorageData fileMetadata, FileSystemNodeStatistics fileSystemNodeStatistics) {
        setId(id);
        setParentId(parentId);
        setName(name);
        setNodeType(type);
        setFileStorageData(fileMetadata);
        setFileSystemNodeStatistics(fileSystemNodeStatistics);
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

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType type) {
        this.nodeType = type;
    }

    public FileStorageData getFileStorageData() {
        return fileStorageData;
    }

    public void setFileStorageData(FileStorageData fileMetadata) {
        this.fileStorageData = fileMetadata;
    }

    public FileSystemNodeStatistics getFileSystemNodeStatistics() {
        return fileSystemNodeStatistics;
    }

    public void setFileSystemNodeStatistics(FileSystemNodeStatistics fileSystemNodeStatistics) {
        this.fileSystemNodeStatistics = fileSystemNodeStatistics;
    }
}

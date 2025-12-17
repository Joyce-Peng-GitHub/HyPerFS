package cn.edu.bit.hyperfs.entity;

public class FileMetaNode {
    public enum NodeType {
        FILE,
        DIRECTORY
    }

    private long id; // 标识该条记录的唯一ID
    private long parentId; // 父节点ID，根节点为null
    private String name; // 文件或文件夹名称
    private NodeType nodeType; // 文件系统节点类型，文件或文件夹
    private FileStorageData storageData; // 文件元数据，文件夹则为null
    private FileStatistics statistics; // 文件统计信息，文件夹则为总和

    public FileMetaNode() {
        this(0, 0, null, null, null, null);
    }

    public FileMetaNode(long id, long parentId, String name, NodeType type,
            FileStorageData storageData, FileStatistics statistics) {
        setId(id);
        setParentId(parentId);
        setName(name);
        setNodeType(type);
        setStorageData(storageData);
        setStatistics(statistics);
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

    public FileStorageData getStorageData() {
        return storageData;
    }

    public void setStorageData(FileStorageData storageData) {
        this.storageData = storageData;
    }

    public FileStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(FileStatistics statistics) {
        this.statistics = statistics;
    }
}

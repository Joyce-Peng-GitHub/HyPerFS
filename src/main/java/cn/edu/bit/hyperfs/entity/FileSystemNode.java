package cn.edu.bit.hyperfs.entity;

enum FileSystemNodeType {
    FILE,
    DIRECTORY
}

public class FileSystemNode {
    private Long id; // 标识该条记录的唯一ID
    private Long parentId; // 父节点ID，根节点为null
    private String name; // 文件或文件夹名称
    private FileSystemNodeType type; // 文件系统节点类型，文件或文件夹
    private FileMetadata fileMetadata; // 文件元数据，文件夹则为null
    private FileSystemNodeStatistics fileSystemNodeStatistics; // 文件统计信息，文件夹则为总和

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FileSystemNodeType getType() {
        return type;
    }

    public void setType(FileSystemNodeType type) {
        this.type = type;
    }

    public FileMetadata getFileMetadata() {
        return fileMetadata;
    }

    public void setFileMetadata(FileMetadata fileMetadata) {
        this.fileMetadata = fileMetadata;
    }

    public FileSystemNodeStatistics getFileSystemNodeStatistics() {
        return fileSystemNodeStatistics;
    }

    public void setFileSystemNodeStatistics(FileSystemNodeStatistics fileSystemNodeStatistics) {
        this.fileSystemNodeStatistics = fileSystemNodeStatistics;
    }
}

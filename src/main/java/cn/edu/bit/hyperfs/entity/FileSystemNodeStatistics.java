package cn.edu.bit.hyperfs.entity;

public class FileSystemNodeStatistics {
    private long uploadTime; // 文件的上传时间或文件夹的最后修改时间
    private int downloadCount; // 文件下载次数或文件夹中文件的总下载次数

    public FileSystemNodeStatistics(long uploadTime, int downloadCount) {
        setUploadTime(uploadTime);
        setDownloadCount(downloadCount);
    }

    public FileSystemNodeStatistics() {
        this(0, 0);
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

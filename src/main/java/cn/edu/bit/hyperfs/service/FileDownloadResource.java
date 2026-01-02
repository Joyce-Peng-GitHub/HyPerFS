package cn.edu.bit.hyperfs.service;

import java.io.File;
import io.netty.channel.DefaultFileRegion;

public record FileDownloadResource(DefaultFileRegion region, String filename, long totalLength, File file) {
}

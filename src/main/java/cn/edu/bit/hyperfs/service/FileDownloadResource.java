package cn.edu.bit.hyperfs.service;

import io.netty.channel.DefaultFileRegion;

public record FileDownloadResource(DefaultFileRegion region, String filename) {
}

package cn.edu.bit.hyperfs.utils;

import java.util.HexFormat;

public class HexUtils {
	private static final HexFormat HEX_FORMAT = HexFormat.of();

	public static String bytesToHex(byte[] bytes) {
		return HEX_FORMAT.formatHex(bytes);
	}

	public static byte[] hexToBytes(String hex) {
		return HEX_FORMAT.parseHex(hex);
	}
}

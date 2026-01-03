package cn.edu.bit.hyperfs.db;

import java.util.List;

public record InsertFileResult(boolean isDuplicated, long id, List<String> deletedHashes) {
}

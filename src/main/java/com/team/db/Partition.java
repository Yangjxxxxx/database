package com.team.db;

import java.io.File;
import java.util.List;

/**
 * 一个分区
 */
public class Partition {

    /**
     * 该分区所在目录
     */
    private final File partitionDirFile;

    /**
     * 分区名称
     */
    private final String partitionName;

    /**
     * 分区值，和分区名称组成 K-V:  partitionName = value.
     */
    private final String value;

    /**
     * 子分区，如果没有，为空列表。
     */
    private final List<Partition> subpartitions;

    public Partition(File partitionDirFile, List<Partition> subpartitions) {
        this.partitionDirFile = partitionDirFile;
        String[] nv = partitionDirFile.getName().split("=");
        partitionName = nv[0];
        value = nv[1];
        this.subpartitions = subpartitions;
    }

    @Override
    public String toString() {
        return "{Name-Value: " + partitionName + "=" + value +
                " , Number of Sub Partition: " + subpartitions.size() +
                "}";
    }

    public File getPartitionDirFile() {
        return partitionDirFile;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getValue() {
        return value;
    }

    public List<Partition> getSubpartitions() {
        return subpartitions;
    }

    /**
     * 本分区是否有子分区。
     */
    public boolean hasSubpartitions() {
        return !subpartitions.isEmpty();
    }

}

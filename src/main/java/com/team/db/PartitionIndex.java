package com.team.db;

import java.util.*;

/**
 * 分区索引，用于快速获取到分区数据。
 */
public class PartitionIndex {

    private final Map<String, Integer> partitionDepthMap = new HashMap<>();

    private final List<List<Partition>> partitionDepth = new ArrayList<>();

    public PartitionIndex(List<Partition> rootPartitions) {
        Queue<Partition> Q = new LinkedList<>(rootPartitions);
        int depth = 0;
        while (!Q.isEmpty()) {
            int size = Q.size();
            String currDepthPartitionName = null;
            List<Partition> currDepthPartitions = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Partition partition = Objects.requireNonNull(Q.poll());
                currDepthPartitionName = partition.getPartitionName();
                if(partition.hasSubpartitions()) {
                    Q.addAll(partition.getSubpartitions());
                }
                currDepthPartitions.add(partition);
            }
            partitionDepthMap.put(currDepthPartitionName, depth);
            partitionDepth.add(currDepthPartitions);
            ++depth;    /* 层数 + 1 */
        }
    }

    @Override
    public String toString() {
        return "PartitionTreeIndex{" +
                "partitionDepthMap=" + partitionDepthMap +
                ", partitionDepth=" + partitionDepth +
                '}';
    }

    /* ====================================== Public ====================================== */

    public
    List<Partition> getDepthPartitions(int index) {
        return partitionDepth.get(index);
    }

    /**
     * 获取某个分区名称的所有分区。
     */
    public
    List<Partition> getDepthPartitions(String partitionName) {
        return partitionDepth.get(partitionDepthMap.get(partitionName));
    }

    public
    int getLevelPartitions() {
        return partitionDepth.size();
    }

}

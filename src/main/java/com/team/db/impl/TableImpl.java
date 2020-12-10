package com.team.db.impl;

import com.team.db.*;
import com.team.util.StringUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 数据库中的一张表。
 */
public class TableImpl implements Table {

    /* ====================================== Fields ====================================== */

    private final File tableDirFile;

    private final List<Partition> partitions;

    private final PartitionIndex partitionIndex;

    public TableImpl(File tableDirFile) {
        this.tableDirFile = tableDirFile;
        /* 加载表中所有的分区，如果有的话 */
        partitions = loadPartitions(tableDirFile);
        /* 加载分区索引。 */
        partitionIndex = new PartitionIndex(partitions);
    }

    public File getTableDirFile() {
        return tableDirFile;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public int getLevelPartitions() {
        return partitionIndex.getLevelPartitions();
    }

    public PartitionIndex getPartitionTreeIndex() {
        return partitionIndex;
    }

    /**
     * 获取表名
     */
    public
    String tableName() {
        return tableDirFile.getName();
    }

    @Override
    public String toString() {
        return "====================================================================" + "\n" +
                "Table Name: " + this.tableName() + "\n" +
                "Level of Partitions: " + getLevelPartitions() + "\n" +
                "Number of Partitions: " + partitions.size() + "\n" +
                "Partitions: " + partitions +
                "\n====================================================================" + "\n\n";
    }

    /* ====================================== Public ====================================== */

    @Override
    public
    int matchesCount(QueryParam queryParam) {

        //无分区
        if(getLevelPartitions() == 0) {
            return this.noPartitionMatchesCount(queryParam);
        }
        /* 参数堆栈化 */
        String cmpColString = queryParam.getCompareColumn();
        QueryParam.CompareType cmpType = queryParam.getCompareType();
        String cmpValue = queryParam.getCompareValue();
        String likeColString = queryParam.getLikeColumn();
        QueryParam.LikeType likeType = queryParam.getLikeType();
        Pattern[] patterns = queryParam.getLikePatterns();


        /* 检查比较参数和 like 参数是列比较还是分区比较。 */
        boolean isCmpCol = cmpColString.contains("column");
        boolean isLikeCol = likeColString.contains("column");


        //有分区
        if(isCmpCol && isLikeCol) {     /* 比较参数和 like 参数都是列 */
            /* 获取比较和 like 匹配列值。 */
            int cmpCol = StringUtil.getColIdx(cmpColString);
            int likeCol = StringUtil.getColIdx(likeColString);
            if(cmpCol == -1 || likeCol == -1){
                System.out.println("StringUtil.getColIdx error");
                System.exit(1);
            }

            /* 通过比较参数完成初步数据筛选。 */
            Queue<Partition> Q = new LinkedList<>(partitions);
            List<String> data = new ArrayList<>(1024);
            while (!Q.isEmpty()) {
                int size = Q.size();
                for (int i = 0; i < size; i++) {
                    Partition partition = Objects.requireNonNull(Q.poll());
                    if( partition.hasSubpartitions() ) {        /* 还不是最终的分区，跳过 */
                        Q.addAll(partition.getSubpartitions()); /* 为了遍历所有分区的数据。 */
                        continue;
                    }

                    /* 遍历这个最终分区 */
                    for (File file : Objects.requireNonNull(partition.getPartitionDirFile().listFiles())) {
                        this.collectDataAfterFilterCmpCol(data, file, cmpType, cmpCol, cmpValue, likeCol);
                    }
                }
            }

            /* 通过 like 匹配参数进行最终筛选。 */
            int matchedCount = 0;
            for (String likeCmpValue : data) {
                if(like(likeCmpValue, likeType, patterns)) {
                    ++matchedCount;
                }
            }

            return matchedCount;
        }

        if(!isCmpCol && isLikeCol) { /* 比较参数是分区， like 参数都是列 */
            /* 获取 like 匹配列值。 */
            int likeCol = StringUtil.getColIdx(likeColString);
            if(likeCol == -1){
                System.out.println("StringUtil.getColIdx error");
                System.exit(1);
            }

            /* 通过比较参数完成初步数据筛选。 */
            List<Partition> initPartitions = partitionIndex.getDepthPartitions(cmpColString);
            List<String> data = new ArrayList<>(initPartitions.size() * 1024);
            for (Partition partition : initPartitions) {
                /* 比较当前分区的值是否匹配，匹配则将当前分区的所有数据加载。 */
                String checkValue = partition.getValue();
                switch (cmpType) {
                    case equals:
                        if (checkValue.equals(cmpValue)) {
                            loadPartitionData(partition, likeCol, data);
                        }
                        break;
                    case notEquals:
                        if (!checkValue.equals(cmpValue)) {
                            loadPartitionData(partition, likeCol, data);
                        }
                        break;
                    case greater:
                        if (checkValue.compareTo(cmpValue) > 0) {
                            loadPartitionData(partition, likeCol, data);
                        }
                        break;
                    case less:
                        if (checkValue.compareTo(cmpValue) < 0) {
                            loadPartitionData(partition, likeCol, data);
                        }
                        break;
                }
            }

            /* 通过 like 匹配参数进行最终筛选。 */
            int matchedCount = 0;
            for (String likeCmpValue : data) {
                if(like(likeCmpValue, likeType, patterns)) {
                    ++matchedCount;
                }
            }

            return matchedCount;
        }

        if(isCmpCol && !isLikeCol) {    /* 比较参数是列, like 参数是分区 */
            int cmpCol = StringUtil.getColIdx(cmpColString);
            if(cmpCol == -1){
                System.out.println("StringUtil.getColIdx error");
                System.exit(1);
            }

            /* 通过比较参数完成初步数据筛选。 */
            List<Partition> initPartitions = partitionIndex.getDepthPartitions(likeColString);
            int matchedCount = 0;
            for (Partition partition : initPartitions) {
                /* 比较 like 参数。 */
                if(!like(partition.getValue(), likeType, patterns)) {
                    continue;
                }

                /* 加载该分区的所有数据。 */
                List<String> data = new ArrayList<>(1024);
                loadPartitionData(partition, cmpCol, data);

                switch (cmpType) {
                    case equals:
                        for (String checkValue : data) {
                            if(checkValue.equals(cmpValue)) {
                                ++matchedCount;
                            }
                        }
                        break;
                    case notEquals:
                        for (String checkValue : data) {
                            if(!checkValue.equals(cmpValue)) {
                                ++matchedCount;
                            }
                        }
                        break;
                    case greater:
                        for (String checkValue : data) {
                            if(checkValue.compareTo(cmpValue) > 0) {
                                ++matchedCount;
                            }
                        }
                        break;
                    case less:
                        for (String checkValue : data) {
                            if(checkValue.compareTo(cmpValue) < 0) {
                                ++matchedCount;
                            }
                        }
                        break;
                }
            }

            return matchedCount;
        }
        /* 比较参数和 like 参数都是分区 */
        System.out.println("Not handler: {isCmpCol: " + false + ", isLikeCol: " + false + "}");
        return -1;
    }

    /* ====================================== Private ====================================== */
    /*
     *表内无分区，返回查询结果
     */
    private
    int noPartitionMatchesCount(QueryParam queryParam){
        /* 参数堆栈化 */
        String cmpColString = queryParam.getCompareColumn();
        QueryParam.CompareType cmpType = queryParam.getCompareType();
        String cmpValue = queryParam.getCompareValue();
        String likeColString = queryParam.getLikeColumn();
        QueryParam.LikeType likeType = queryParam.getLikeType();
        Pattern[] patterns = queryParam.getLikePatterns();


        /* 检查比较参数和 like 参数是列比较还是分区比较。 */
        boolean isCmpCol = cmpColString.contains("column");
        boolean isLikeCol = likeColString.contains("column");

        File[] dataFiles;
        if ((dataFiles = tableDirFile.listFiles()) == null) {
            return 0;
        }

        /* 对于无分区表，两个比较只能是列，获取比较和 like 匹配列值。 */
        int cmpCol = StringUtil.getColIdx(cmpColString);
        int likeCol = StringUtil.getColIdx(likeColString);
        if(cmpCol == -1 || likeCol == -1){
            System.out.println("StringUtil.getColIdx error");
            System.exit(1);
        }

        /* 通过比较参数完成初步数据筛选。 */
        List<String> data = new ArrayList<>(1024);
        for (File file : dataFiles) {
            collectDataAfterFilterCmpCol(data, file, cmpType,
                    cmpCol, cmpValue, likeCol);
        }

        /* 通过 like 匹配参数进行第最终筛选。 */
        int matchedCount = 0;
        for (String likeCmpValue : data) {
            if (like(likeCmpValue, likeType, patterns)) {
                ++matchedCount;
            }
        }

        return matchedCount;
    }

    /*
     *过滤匹配列，收集like列的数据，下一步like使用
     */
    private void collectDataAfterFilterCmpCol(List<String> data, File file, QueryParam.CompareType cmpType,
                                              int cmpCol, String cmpValue, int likeCol){
        CSVFile csvFile = new CSVFile(file);
        int rowIndex = 0;

        switch (cmpType) {
            case equals:
                while (csvFile.checkRowIndex(rowIndex)) {
                    if (csvFile.get(rowIndex, cmpCol).equals(cmpValue)) {
                        data.add(csvFile.get(rowIndex, likeCol));
                    }
                    rowIndex++;
                }
                break;
            case notEquals:
                while (csvFile.checkRowIndex(rowIndex)) {
                    if (!csvFile.get(rowIndex, cmpCol).equals(cmpValue)) {
                        data.add(csvFile.get(rowIndex, likeCol));
                    }
                    rowIndex++;
                }
                break;
            case greater:
                while (csvFile.checkRowIndex(rowIndex)) {
                    if (csvFile.get(rowIndex, cmpCol).compareTo(cmpValue) > 0) {
                        data.add(csvFile.get(rowIndex, likeCol));
                    }
                    rowIndex++;
                }
                break;
            case less:
                while (csvFile.checkRowIndex(rowIndex)) {
                    if (csvFile.get(rowIndex, cmpCol).compareTo(cmpValue) < 0) {
                        data.add(csvFile.get(rowIndex, likeCol));
                    }
                    rowIndex++;
                }
                break;
        }
    }

    /**
     * 加载分区的某列数据到 ${data} 列表中。
     */
    private
    void loadPartitionData(Partition partition, int likeCol, List<String> data) {
        Queue<Partition> Q = new LinkedList<>();
        Q.add(partition);
        while (!Q.isEmpty()) {
            int size = Q.size();
            for (int i = 0; i < size; i++) {
                Partition p = Objects.requireNonNull(Q.poll());
                if(p.hasSubpartitions()) {
                    Q.addAll(p.getSubpartitions());
                    continue;
                }

                /* 加载最终分区的所有数据。 */
                for (File file : Objects.requireNonNull(p.getPartitionDirFile().listFiles())) {
                    CSVFile csvFile = new CSVFile(file);
                    Integer rowIndex = 0;
                    while(csvFile.checkRowIndex(rowIndex)){
                        data.add(csvFile.get(rowIndex, likeCol));
                        rowIndex++;
                    }
                }
            }
        }
    }

    /**
     * 模糊匹配
     * 支持 all_like, any_like, none_like
     */
    private
    boolean like(String cmpValue, QueryParam.LikeType likeType, Pattern... patterns) {
        switch (likeType) {
            case allLike:
                return StringUtil.allLike(cmpValue, patterns);
            case anyLike:
                return StringUtil.anyLike(cmpValue, patterns);
            case noneLike:
                return StringUtil.noneLike(cmpValue, patterns);
        }
        throw new RuntimeException("!!!!" + likeType.toString());
    }

    /**
     * 递归加载分区。
     */
    private
    List<Partition> loadPartitions(File partitionDir) {
        if(partitionDir == null || !partitionDir.exists() || !partitionDir.isDirectory()) {
            return Collections.emptyList();
        }

        File[] files;
        if( (files = partitionDir.listFiles()) == null || files.length == 0 ) {
            return Collections.emptyList();
        }

        /* 加载该目录下的所有分区表 */
        List<Partition> partitions = new ArrayList<>(files.length);
        for (File file : files) {
            if(file.isDirectory()) {
                partitions.add(new Partition(file, loadPartitions(file)));
            }
        }

        return partitions;
    }

}

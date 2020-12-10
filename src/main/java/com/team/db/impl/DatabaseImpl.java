package com.team.db.impl;

import com.team.db.Database;
import com.team.db.QueryParam;
import com.team.db.Table;
import com.team.Main;

import java.io.File;
import java.util.*;

public class DatabaseImpl implements Database {

    /* ====================================== Fields ====================================== */

    /**
     * 数据集所在目录
     */
    private final File dbDirFile;

    /**
     * 所有表
     */
    private final Map<String, Table> tables;

    private DatabaseImpl(File dbDirFile, Map<String, Table> tables) {
        this.dbDirFile = dbDirFile;
        this.tables = tables;
    }

    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(1024);
        for (Table table : tables.values()) {
            builder.append(table.toString());
        }
        return builder.toString();
    }

    /* ====================================== Public ====================================== */

    @Override
    public List<String> query(String param) {
        return new ArrayList<>(0);
    }

    @Override
    public int matchesCount(String param) {
        QueryParam queryParam = QueryParam.valueOf(param);
        if(Main.LOCAL) {
            System.out.println(queryParam);
            System.out.println();
        }

        return tables.get(queryParam.getTable()).matchesCount(queryParam);
    }

    public static
    Database load(String dbDir) {
        /* 加载所有表 */
        File dbDirFile;
        if( !(dbDirFile = new File(dbDir)).exists() || !dbDirFile.isDirectory() ) {
            return null;
        }

        File[] tableDirFiles;
        if( (tableDirFiles = dbDirFile.listFiles()) == null || tableDirFiles.length == 0 ) {
            return new DatabaseImpl(dbDirFile, Collections.emptyMap());
        }

        Map<String, Table> tables = new HashMap<>(tableDirFiles.length);
        for (File tableDirFile : tableDirFiles) {
            if(tableDirFile.isDirectory()) {
                tables.put(tableDirFile.getName(), new TableImpl(tableDirFile));
            }
        }

        return new DatabaseImpl(dbDirFile, tables);
    }

}

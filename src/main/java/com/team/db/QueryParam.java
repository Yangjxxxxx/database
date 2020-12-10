package com.team.db;


import com.team.util.StringUtil;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * 查询参数。
 *
 * 输入格式： table0 column0 > '200000' column2 ANY_LIKE ['2017%','2018%','2019%']
 */
public class QueryParam {

    /* ====================================== Fields ====================================== */

    /**
     * 表名：字符串，表示定位一个表名（一定存在）
     */
    private final String table;

    /**
     * 比较列：字符串，表示该表的某个列名（包括数据列和分区列，一定存在），用来做数据粗筛
     */
    private final String compareColumn;

    /**
     * 操作符：取值范围'>','<','=','!='，表示${比较列}需要满足一定的筛选条件，注意统一满足字符串的比较语义
     */
    private final CompareType compareType;

    /**
     * 比较值：字符串，表示${比较列}满足一定条件所对应的比较值
     */
    private final String compareValue;

    /**
     * like匹配列：字符串，用来做like语义的列名（一定存在）
     */
    private final String likeColumn;

    /**
     * like类型：字符串，
     *  - 'ALL_LIKE'表示所有模式串都能匹配才成功;
     *  - 'ANY_LIKE'表示任意一个模式串匹配就成功;
     *  - 'NONE_LIKE'表示任意一个模式串都不匹配才成功;
     */
    private final LikeType likeType;

    /**
     * like匹配值数组：不定长的like匹配字符串列表，其中模式串个数 n > 0 && n <= 10
     */
    private final String[] likeParams;

    private QueryParam(String table, String compareColumn, CompareType compareType, String compareValue,
                      String likeColumn, LikeType likeType, String[] likeParams) {
        this.table = table;
        this.compareColumn = compareColumn;
        this.compareType = compareType;
        this.compareValue = compareValue;
        this.likeColumn = likeColumn;
        this.likeType = likeType;
        this.likeParams = likeParams;
    }

    public static
    QueryParam valueOf(String param) {
        if(param == null || param.isEmpty()) {
            return null;
        }
        String[] paramSplit = param.split(" ");

        return new QueryParam(
                paramSplit[0],
                paramSplit[1],
                CompareType.getTypeByString(paramSplit[2]),
                StringUtil.standardValueString(paramSplit[3]),
                paramSplit[4],
                LikeType.getTypeByString(paramSplit[5]),
                StringUtil.arrayStringToArray(paramSplit[6]));
    }

    @Override
    public String toString() {
        return "QueryParam{" +
                "table='" + table + '\'' +
                ", compareColumn='" + compareColumn + '\'' +
                ", compareType=" + compareType +
                ", compareValue='" + compareValue + '\'' +
                ", likeColumn='" + likeColumn + '\'' +
                ", likeType=" + likeType +
                ", likeParams=" + Arrays.toString(likeParams) +
                '}';
    }

    public String getTable() {
        return table;
    }

    public String getCompareColumn() {
        return compareColumn;
    }

    public CompareType getCompareType() {
        return compareType;
    }

    public String getCompareValue() {
        return compareValue;
    }

    public String getLikeColumn() {
        return likeColumn;
    }

    public LikeType getLikeType() {
        return likeType;
    }

    public String[] getLikeParams() {
        return likeParams;
    }

    /**
     * 获取 like 参数对应的正则表达式对象数组。
     */
    public Pattern[] getLikePatterns() {
        Pattern[] patterns = new Pattern[likeParams.length];
        for (int i = 0; i < likeParams.length; i++) {
            patterns[i] = Pattern.compile(StringUtil.likeMatchesStringToRegex(likeParams[i]), Pattern.DOTALL);
        }
        return patterns;
    }

    /* ====================================== Supports ====================================== */

    /**
     * 比较操作符类型
     */
    public enum CompareType {
        greater(">"),
        less("<"),
        equals("="),
        notEquals("!=");

        private final String typeString;

        CompareType(String typeString) {
            this.typeString = typeString;
        }

        @Override
        public
        String toString() {
            return typeString;
        }

        public static
        CompareType getTypeByString(String typeString) {
            for (CompareType compareType : CompareType.values()) {
                if(compareType.toString().equals(typeString)) {
                    return compareType;
                }
            }
            return null;
        }
    }

    /**
     * like类型
     */
    public enum LikeType {
        allLike("ALL_LIKE"),
        anyLike("ANY_LIKE"),
        noneLike("NONE_LIKE");

        private final String typeString;

        LikeType(String typeString) {
            this.typeString = typeString;
        }

        @Override
        public
        String toString() {
            return typeString;
        }

        public static
        LikeType getTypeByString(String typeString) {
            for (LikeType likeType : LikeType.values()) {
                if(likeType.toString().equals(typeString)) {
                    return likeType;
                }
            }
            return null;
        }

    }


}

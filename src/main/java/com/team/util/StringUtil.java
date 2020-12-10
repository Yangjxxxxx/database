package com.team.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 字符串实用程序
 */
public class StringUtil {

    /**
     * Eg.
     *  'xxyy' -> xxyy
     *  'xxyy\'dd' -> xxyy\'dd
     */
    public static
    String standardValueString(String value) {
        if(value == null || value.isEmpty()) {
            return "";
        }

        return new String(value.toCharArray(), 1, value.length() - 2);
    }

    /**
     * Eg.
     *  "['2017%','2018%','2019%']" -> List{['1996%','1997%','1998%']}
     */
    public static
    List<String> arrayStringToList(String arrayString) {
        if(arrayString == null || arrayString.isEmpty()) {
            return Collections.emptyList();
        }

        arrayString = new String(arrayString.toCharArray(), 1, arrayString.length() - 2);
        return Arrays.stream(arrayString.split(","))
                .map(StringUtil::standardValueString).collect(Collectors.toList());
    }

    /**
     * Eg.
     *  "['2017%','2018%','2019%']" -> ["1996%","1997%","1998%"]
     */
    public static
    String[] arrayStringToArray(String arrayString) {
        if(arrayString == null || arrayString.isEmpty()) {
            return new String[0];
        }

        arrayString = new String(arrayString.toCharArray(), 1, arrayString.length() - 2);
        String[] split = arrayString.split(",");
        String[] ans = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            ans[i] = StringUtil.standardValueString(split[i]);
        }
        return ans;
    }


    /**
     * Eg.
     *  '%love%' -> '.*love.*'
     *  '%.%.%.%' -> '.*\..*\..*\..*'
     *  '\%%' -> '%.*'
     */
    public static
    String likeMatchesStringToRegex(String text) {
        text = text.replace("\\%", "${M}");
        text = text.replace("\\_", "${S}");

        text = text.replace(".", "\\.")
                .replace("*", "\\*")
                .replace("%", ".*")
                .replace("_", ".");

        return text.replace("${M}", "%")
                .replace("${S}", "_");
    }

    /**
     * 所有模式串都能匹配才成功。
     */
    public static
    boolean allLike(String cmpValue, Pattern... patterns) {
        for (Pattern pattern : patterns) {
            if(!pattern.matcher(cmpValue).matches()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 任意一个模式串匹配就成功。
     */
    public static
    boolean anyLike(String cmpValue, Pattern... patterns) {
        for (Pattern pattern : patterns) {
            if(pattern.matcher(cmpValue).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 任意一个模式串都不匹配才成功。
     */
    public static
    boolean noneLike(String cmpValue, Pattern... patterns) {
        for (Pattern pattern : patterns) {
            if(pattern.matcher(cmpValue).matches()) {
                return false;
            }
        }
        return true;
    }

    public static
    Integer getColIdx(String value) {
        if(!value.contains("column")) return -1;
        Integer index = 0;
        for(int i = 6; i < value.length(); ++i){
            index = index*10 + (value.charAt(i) - '0');
        }
        return index;
    }

}

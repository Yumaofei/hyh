package com.yumaofei.UDF;

import com.alibaba.fastjson2.JSONArray;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @program: hyh
 * @description: 将字符串类型转换成json数组，字符串必须满足格式条件
 * @author: Mr.YMF
 * @create: 2023-04-25 15:50
 **/
import java.util.List;

public final class String2JsonArray extends UDF {
    public static List<String> evaluate(String sourceText) {
        int strLen;
        if (sourceText != null && (strLen = sourceText.length()) != 0)
            for(int i = 0; i < strLen; ++i)
                if (!Character.isWhitespace(sourceText.charAt(i)))
                    return null;

        if ("null".equalsIgnoreCase(sourceText)) {
            return null;
        }

        JSONArray jsonArray = JSONArray.parseArray(sourceText);
        List<String> arrayList = jsonArray.toList(String.class);

        return arrayList;
    }
}
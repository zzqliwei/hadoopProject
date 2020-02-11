package com.westar.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ReverseString extends UDF {
    public Text evaluate(final Text text){
        if(null == text){
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder(text.toString());
        String reverseStr = stringBuilder.reverse().toString();
        return new Text(reverseStr);

    }
}

package com.yumaofei;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KuduSink<IN> extends RichSinkFunction<String> implements Serializable {
    private static final long serialVersionUID = 1L;
    private KuduUtil kuduUtil;
    private static String table_name;
    private static String keynums;

    @Override
    public void open(Configuration param) throws Exception
    {
        super.open(param);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        table_name=parameterTool.get("kudu.table.name");
        keynums=parameterTool.get("kudu.table.keynums");
        this.kuduUtil = new KuduUtil(table_name);
    }

    @Override
    public void invoke(String s) throws IOException{
        if ((s.contains("\n")) || (s.contains("\r")) || (s.contains("\r\n"))) {
            s = s.trim();
        }
        List<String> lines=new ArrayList<>(Arrays.asList(s.split("\n",-1)));
        for(String line:lines){
            //xxx为分隔符
            List<String> strs = new ArrayList<>(Arrays.asList(line.split("xxx", -1)));
            String op_type = strs.get(0);
            //这里可以写一些业务逻辑
            if (op_type.toUpperCase().equals("I")) {
                this.kuduUtil.insert(strs);
            } else if (op_type.toUpperCase().equals("U")) {
                this.kuduUtil.update(strs);
            } else if (op_type.toUpperCase().equals("D")) {
                this.kuduUtil.delete(strs.subList(0,Integer.parseInt(keynums)));
            }
        }
    }


    @Override
    public void close()
            throws Exception
    {
        this.kuduUtil.close();
    }

}



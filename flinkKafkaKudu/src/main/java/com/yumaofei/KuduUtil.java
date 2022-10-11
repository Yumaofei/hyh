package com.yumaofei;

import org.apache.kudu.client.*;

import java.util.List;

public class KuduUtil {

    private static KuduClient client;
    private static KuduTable table;
    private static KuduSession session;

    public KuduUtil(String table_name) throws KuduException {
        //这里填写kudu的ip及端口
        String hosts = "hdp:8051";
        String tableName=table_name;
        client = new KuduClient.KuduClientBuilder(hosts).build();
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(20000);
        table = client.openTable(tableName);
    }

    public void insert(List<String> columns) throws KuduException {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        for (int i = 0; i < columns.size(); i++) {
            row.addString(i,columns.get(i));
        }
        session.apply(insert);
        session.flush();
    }

    public void update(List<String> columns) throws KuduException {
        Update update = table.newUpdate();
        PartialRow row = update.getRow();
        for(int i=0;i<columns.size();i++) {
            row.addString(i,columns.get(i));
        }
        session.apply(update);
        session.flush();
    }

    public void delete(List<String> columns) throws KuduException {
        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();
        for(int i=0;i<columns.size();i++){
            row.addString(i,columns.get(i));
        }
        session.apply(delete);
        session.flush();
    }

    public void close()
            throws KuduException {
        if (null != session) {
            session.flush();
            session.close();
        }
        if (null != client) {
            client.shutdown();
        }
    }

}



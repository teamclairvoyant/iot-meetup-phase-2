package com.clairvoyantsoft.flink.Utils;

import org.apache.kudu.client.KuduException;

import com.clairvoyantsoft.flink.Utils.Exceptions.KuduClientException;


public class ReadKuduTable {

    public static void main(String[] args) {

        String table = ""; // TODO insert table name
        String host = "localhost";

        try {
            Utils utils = new Utils(host);
            utils.readTablePrint(table);
        } catch (KuduClientException e) {
            e.printStackTrace();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
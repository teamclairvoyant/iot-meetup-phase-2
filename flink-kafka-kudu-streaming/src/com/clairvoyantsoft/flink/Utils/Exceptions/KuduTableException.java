package com.clairvoyantsoft.flink.Utils.Exceptions;

import java.io.IOException;

public class KuduTableException extends IOException {

    public KuduTableException (String msg){
        super(msg);
    }
}

package com.clairvoyantsoft.flink.Utils.Exceptions;

import java.io.IOException;

public class KuduClientException extends IOException {

    public KuduClientException (String msg) {
        super(msg);
    }
}

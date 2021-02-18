package com.baletu.datasync.client;

public class DataClientException extends RuntimeException {

    private static final long serialVersionUID = -7545341502620139031L;

    public DataClientException(String errorCode){
        super(errorCode);
    }

    public DataClientException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public DataClientException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public DataClientException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public DataClientException(Throwable cause){
        super(cause);
    }
}

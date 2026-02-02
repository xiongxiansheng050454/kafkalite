package com.kafkalite.server.protocol;

public enum  RequestType {

    PRODUCE((byte) 0x01),
    FETCH((byte) 0x02),
    METADATA((byte) 0x03),
    UNKNOWN((byte) 0xFF);

    private final byte code;

    RequestType(byte code) { this.code = code; }

    public byte code() { return code; }

    public static RequestType fromCode(byte code) {
        for (RequestType type : values()) {
            if (type.code() == code) return type;
        }
        return UNKNOWN;
    }
}

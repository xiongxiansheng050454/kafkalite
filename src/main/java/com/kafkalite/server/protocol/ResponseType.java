package com.kafkalite.server.protocol;

public enum ResponseType {
    PRODUCE((byte) 0x01),
    FETCH((byte) 0x02),
    METADATA((byte) 0x03),
    UNKNOWN((byte) 0xFF);

    private final byte code;
    ResponseType(byte code) { this.code = code; }
    public byte code() { return code; }

    public static ResponseType fromCode(byte code) {
        for (ResponseType t : values()) {
            if (t.code() == code) return t;
        }
        return UNKNOWN;
    }
}
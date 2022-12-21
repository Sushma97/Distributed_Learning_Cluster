package com.cs425.membership;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;

public class ObjectSize {
    public static int sizeInBytes(Serializable obj) {
        return SerializationUtils.serialize(obj).length;
    }
}

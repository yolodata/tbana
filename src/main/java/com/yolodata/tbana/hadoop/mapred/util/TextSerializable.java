package com.yolodata.tbana.hadoop.mapred.util;

import org.apache.hadoop.io.Text;

import java.io.Serializable;

public class TextSerializable extends Text implements Serializable {
    public TextSerializable() {
        super();
    }

    public TextSerializable(String s) {
        super(s);
    }
}

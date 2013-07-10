package com.yolodata.tbana.hadoop.mapred.shuttl.index;

import org.apache.hadoop.fs.Path;

import java.io.File;

public class Index {

    private String path;
    private String name;

    public Index(String path) {
        this(path, (new File(path)).getName());
    }

    public Index(Path path) {
        this.path = path.toString();
        this.name = path.getName();
    }

    public Index(String path, String name) {
        this.path = path;
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || obj.getClass() != this.getClass())
            return false;

        Index i = (Index) obj;
        return i.name == this.name && i.path == this.path;
    }
}

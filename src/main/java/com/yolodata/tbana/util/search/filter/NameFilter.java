package com.yolodata.tbana.util.search.filter;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class NameFilter implements SearchFilter{

    private String [] names;

    public NameFilter(String name) {

        this.names = new String[] {name};
    }

    public NameFilter(String[] names) {
        this.names = names;
    }

    @Override
    public boolean accept(String path) throws IOException {
        Path p = new Path(path);

        return matchesAnyName(p.getName());
    }

    private boolean matchesAnyName(String name) {
        for(String n : names) {
            if(n.equals(name))
                return true;
        }

        return false;
    }


}

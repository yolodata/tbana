package com.yolodata.tbana.util.search.filter;

import java.io.IOException;

public interface SearchFilter {
    public boolean accept(String path) throws IOException;
}

package com.yolodata.tbana.util.search;

import com.yolodata.tbana.util.search.filter.SearchFilter;

import java.io.IOException;
import java.util.List;

public interface PathFinder {

    public List<String> findPaths(String rootPath) throws IOException;
    public List<String> findPaths(String rootPath, List<SearchFilter> filters) throws IOException;
}

/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.util.search;

import com.yolodata.tbana.util.search.filter.SearchFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopPathFinder implements PathFinder {

    private FileSystem fileSystem;
    private int maxResults;

    public HadoopPathFinder(FileSystem fs) {
        this(fs,0);
    }

    public HadoopPathFinder(FileSystem fs,int maxResults) {
        this.fileSystem = fs;
        this.maxResults = maxResults;
    }

    @Override
    public List<String> findPaths(String rootPath) throws IOException {
        return findPaths(rootPath,new ArrayList<SearchFilter>(){});
    }

    @Override
    public List<String> findPaths(String rootPath, List<SearchFilter> filters) throws IOException {
        FileStatus path = fileSystem.getFileStatus(new Path(rootPath));

        List<String> result = new ArrayList<String>();

        if(path.isDir())
            result.addAll(findAllPathsInDir(path.getPath(), filters));
        else if(pathPassesFilters(path,filters))
            result.add(rootPath);

        if(maxResults>0 && maxResults-result.size() <= 0)
            return result.subList(0,maxResults);

        return result;
    }

    private List<String> findAllPathsInDir(Path directory, List<SearchFilter> filters) throws IOException {
        List<String> result = new ArrayList<String>();

        FileStatus [] listFiles = fileSystem.listStatus(directory);
        for(FileStatus file : listFiles)
            if(pathPassesFilters(file,filters))
                result.add(file.getPath().toString());

        return result;
    }

    private boolean pathPassesFilters(FileStatus file, List<SearchFilter> filters) throws IOException {
        for(SearchFilter filter : filters)
            if(!filter.accept(file.getPath().toString()))
                return false;
        return true;
    }
}

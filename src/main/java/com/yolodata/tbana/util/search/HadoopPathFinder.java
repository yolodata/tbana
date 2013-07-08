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

    public HadoopPathFinder(FileSystem fs) {
        this.fileSystem = fs;
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
        else
            if(pathPassesFilters(path,filters))
                result.add(rootPath);

        return result;
    }

    private List<String> findAllPathsInDir(Path directory, List<SearchFilter> filters) throws IOException {
        List<String> result = new ArrayList<String>();

        FileStatus [] listFiles = fileSystem.listStatus(directory);
        for(FileStatus file : listFiles)
            if(pathPassesFilters(file, filters))
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

package com.yolodata.tbana.hadoop.mapred.shuttl.index;

import com.yolodata.tbana.util.search.HadoopPathFinder;
import com.yolodata.tbana.util.search.filter.DirectoryFilter;
import com.yolodata.tbana.util.search.filter.NameFilter;
import com.yolodata.tbana.util.search.filter.SearchFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexFinder {
    private FileSystem fileSystem;
    private final Path root;

    public IndexFinder(FileSystem fileSystem, Path root) {
        this.fileSystem = fileSystem;
        this.root = root;
    }

    public List<Index> find() throws IOException {
        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new DirectoryFilter(fileSystem));
        return find(new ArrayList<SearchFilter>());
    }

    public List<Index> find(String [] indexName) throws IOException {
        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new NameFilter(indexName));
        filters.add(new DirectoryFilter(fileSystem));
        return find(filters);
    }

    private List<Index> find(List<SearchFilter> filters) throws IOException {
        HadoopPathFinder finder = new HadoopPathFinder(fileSystem);

        List<Index> indexes = new ArrayList<Index>();
        for(String cluster : finder.findPaths(root.toString()))
            for(String server : finder.findPaths(cluster))
                for(String indexer : finder.findPaths(server))
                    for(String index : finder.findPaths(indexer,filters))
                    {
                        Path indexPath = new Path(index);
                        indexes.add(new Index(indexPath));
                    }

        return indexes;
    }
}

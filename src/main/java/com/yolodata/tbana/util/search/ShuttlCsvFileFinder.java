package com.yolodata.tbana.util.search;

import com.splunk.shuttl.archiver.model.Bucket;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.shuttl.bucket.BucketFinder;
import com.yolodata.tbana.hadoop.mapred.shuttl.index.Index;
import com.yolodata.tbana.hadoop.mapred.shuttl.index.IndexFinder;
import com.yolodata.tbana.util.search.filter.ExtensionFilter;
import com.yolodata.tbana.util.search.filter.SearchFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ShuttlCsvFileFinder {

    private FileSystem fileSystem;
    private Path shuttlRoot;

    public ShuttlCsvFileFinder(FileSystem fileSystem, Path shuttlRoot) {

        this.fileSystem = fileSystem;
        this.shuttlRoot = shuttlRoot;
    }

    public List<String> findFiles(SplunkDataQuery query) throws IOException {
        return findFiles(query,0);
    }

    public Path findSingleFile(SplunkDataQuery query) throws IOException {
        String path = findFiles(query, 1).get(0);
        return new Path(path);
    }

    public List<String> findFiles(SplunkDataQuery query, int limit) throws IOException {
        IndexFinder indexFinder = new IndexFinder(fileSystem, shuttlRoot);
        List<Index> indexList = indexFinder.find(query.getIndexes());
        List<Bucket> bucketList = (new BucketFinder(fileSystem,indexList,1)).search(query);
        List<String> pathFinder = (new HadoopPathFinder(fileSystem)).findPaths(bucketList.get(0).getPath(), Arrays.asList((SearchFilter) (new ExtensionFilter("csv"))));

        return pathFinder;

    }
}

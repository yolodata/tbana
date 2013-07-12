package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import com.splunk.shuttl.archiver.model.Bucket;
import com.yolodata.tbana.cascading.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.shuttl.index.Index;
import com.yolodata.tbana.util.search.HadoopPathFinder;
import com.yolodata.tbana.util.search.PathFinder;
import com.yolodata.tbana.util.search.filter.BucketFilter;
import com.yolodata.tbana.util.search.filter.BucketTimestampFilter;
import com.yolodata.tbana.util.search.filter.SearchFilter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BucketFinder {

    private FileSystem fileSystem;
    private List<Index> indexes;
    private int maxResults;

    public BucketFinder(FileSystem fs, Index index) {
        this(fs,index,0);
    }

    public BucketFinder(FileSystem fs, Index index, int maxResults) {
        this(fs, Arrays.asList(index),maxResults);
    }

    public BucketFinder(FileSystem fs, List<Index> indexes) {
        this(fs,indexes,0);
    }

    public BucketFinder(FileSystem fs, List<Index> indexes, int maxResults) {
        this.fileSystem = fs;
        this.indexes = indexes;
        this.maxResults = maxResults;
    }

    public List<Bucket> search() throws IOException {
        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new BucketFilter(fileSystem));
        return search(filters);
    }

    public List<Bucket> search(SplunkDataQuery splunkSearch) throws IOException {

        long earliest = TimeParser.parse(splunkSearch.getEarliestTime());
        long latest = TimeParser.parse(splunkSearch.getLatestTime());

        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new BucketTimestampFilter(fileSystem,earliest,latest));

        return search(filters);
    }

    public List<Bucket> search(List<SearchFilter> filters) throws IOException {

        List<Bucket> buckets = new ArrayList<Bucket>();

        PathFinder finder = new HadoopPathFinder(fileSystem,maxResults);

        for(Index index : indexes) {
            List<String> bucketPaths = finder.findPaths(index.getPath(), filters);

            for(String bucketPath : bucketPaths) {
                if(maxResults > 0 && buckets.size()-maxResults == 0)
                    return buckets;
                buckets.add(HadoopBucketFactory.createUsingPathToBucket(bucketPath, index));
            }
        }
        return buckets;
    }
}

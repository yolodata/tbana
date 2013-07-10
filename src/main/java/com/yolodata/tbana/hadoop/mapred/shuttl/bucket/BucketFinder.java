package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import com.yolodata.tbana.cascading.splunk.SplunkDataQuery;
import com.yolodata.tbana.util.search.HadoopPathFinder;
import com.yolodata.tbana.util.search.PathFinder;
import com.yolodata.tbana.util.search.filter.BucketFilter;
import com.yolodata.tbana.util.search.filter.BucketTimestampFilter;
import com.yolodata.tbana.util.search.filter.SearchFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class BucketFinder {

    private FileSystem fileSystem;
    private Path shuttlHome;

    private List<SearchFilter> filters;

    public BucketFinder(FileSystem fs, Path shuttlHome) {
        this.fileSystem = fs;
        this.shuttlHome = shuttlHome;

        filters = getDefaultBucketFilters();
    }

    private List<SearchFilter> getDefaultBucketFilters() {
        List<SearchFilter> result = new ArrayList<SearchFilter>();

        result.add(new BucketFilter(fileSystem));

        return result;
    }

    public List<Bucket> search(SplunkDataQuery splunkSearch) throws ParseException, IOException {

        List<Bucket> buckets = new ArrayList<Bucket>();

        long earliest = TimeParser.parse(splunkSearch.getEarliestTime());
        long latest = TimeParser.parse(splunkSearch.getLatestTime());

        filters.clear();
        filters.add(new BucketTimestampFilter(fileSystem,earliest,latest));

        PathFinder finder = new HadoopPathFinder(fileSystem);
        List<String> bucketPaths = finder.findPaths(shuttlHome.toString(), filters);

        for(String bucketPath : bucketPaths) {
            buckets.add(Bucket.create(bucketPath));
        }

        return buckets;
    }
}

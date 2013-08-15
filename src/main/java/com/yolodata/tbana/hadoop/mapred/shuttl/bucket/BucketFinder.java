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

package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import com.splunk.shuttl.archiver.model.Bucket;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkDataQuery;
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

        long earliest = splunkSearch.getEarliestTime().getMillis();
        long latest = splunkSearch.getLatestTime().getMillis();

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

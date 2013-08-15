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

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShuttlDirectoryTreeFactory {

    private List<Path> clusterPaths;
    private List<Path> serverPaths;
    private List<Path> indexerPaths;
    private List<Path> indexPaths;
    private final FileSystem fileSystem;
    private final Path root;

    public ShuttlDirectoryTreeFactory() throws IOException {
        fileSystem = FileSystem.getLocal(new Configuration());
        root = FileSystemTestUtils.createEmptyDir(fileSystem);
        indexPaths = new ArrayList<Path>();
        createBaseStructure();
    }

    public Path getRoot() {
        return root;
    }

    public void remove() throws IOException {
        fileSystem.delete(root,true);
    }

    private void createBaseStructure() throws IOException {
        String [] clusters = {"cluster-1", "cluster-2"};
        clusterPaths = FileSystemTestUtils.createDirectories(fileSystem, root, clusters);

        String [] servers = {"server-1", "server-2"};
        serverPaths = FileSystemTestUtils.createDirectories(fileSystem, clusterPaths.get(0), servers);

        String [] indexers = {"indexer-1", "indexer-2"};
        indexerPaths = FileSystemTestUtils.createDirectories(fileSystem, serverPaths.get(0), indexers);
    }

    public Path addIndex(Path indexerPath, String indexName) throws IOException {
        Path indexPath = FileSystemTestUtils.createEmptyDir(fileSystem, indexerPath, indexName);
        indexPaths.add(indexPath);
        return indexPath;
    }

    public Path addBucket(Path indexPath, String bucketName) throws IOException {
        return FileSystemTestUtils.createEmptyDir(fileSystem,indexPath,bucketName);
    }

    public List<Path> getIndexerPaths() {
        return indexerPaths;
    }
}

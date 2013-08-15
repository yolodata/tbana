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

package com.yolodata.tbana.hadoop.mapred.shuttl.index;

import com.yolodata.tbana.util.search.ShuttlDirectoryTreeFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class IndexFinderTest {

    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = FileSystem.getLocal(new Configuration());
    }

    @Test
    public void testFinder() throws Exception {

        ShuttlDirectoryTreeFactory directoryTreeFactory = new ShuttlDirectoryTreeFactory();
        List<Path> indexers = directoryTreeFactory.getIndexerPaths();

        directoryTreeFactory.addIndex(indexers.get(0),"index1");
        directoryTreeFactory.addIndex(indexers.get(1),"index2");

        IndexFinder finder = new IndexFinder(fileSystem,directoryTreeFactory.getRoot());

        List<Index> indexes = finder.find();


        assertEquals(indexes.size(), 2);
        assertEquals(indexes.get(0).getName(), "index1");
        assertEquals(indexes.get(1).getName(), "index2");
    }
}

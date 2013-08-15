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

package com.yolodata.tbana.util.search.filter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectoryFilterTest {

    private LocalFileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = mock(LocalFileSystem.class);
    }

    @Test
    public void testFilterOnCorrectData() throws Exception {
        DirectoryFilter filter = new DirectoryFilter(fileSystem);

        FileStatus directoryStatus = new FileStatus(0, true, 0, 0, 0, null);
        FileStatus fileStatus = new FileStatus(0, false, 0, 0, 0, null);

        Path directory = new Path("directory");
        Path file = new Path("file");

        when(fileSystem.getFileStatus(directory)).thenReturn(directoryStatus);
        when(fileSystem.getFileStatus(file)).thenReturn(fileStatus);

        assertEquals(true, filter.accept(directory));
        assertEquals(false, filter.accept(file));

    }


}

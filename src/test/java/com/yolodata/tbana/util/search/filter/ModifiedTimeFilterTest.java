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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModifiedTimeFilterTest {

    @Test
    public void testFilter() throws Exception {

        FileSystem fs = mock(FileSystem.class);

        // We want everything newer than 100, i.e. modified times over 100 are accepted
        ModifiedTimeFilter filter = new ModifiedTimeFilter(fs, 100);

        Path anyPath = createMockPathWithModifiedTime(fs, 1000);
        assertEquals(true, filter.accept(anyPath));

        anyPath = createMockPathWithModifiedTime(fs, 90);
        assertEquals(false, filter.accept(anyPath));

        anyPath = createMockPathWithModifiedTime(fs, 100);
        assertEquals(false, filter.accept(anyPath));
    }

    private Path createMockPathWithModifiedTime(FileSystem fs, long modifiedTime) throws IOException {
        Path anyPath = new Path("anyPath");
        FileStatus status = new FileStatus(0,false,0,0,modifiedTime,anyPath);
        when(fs.getFileStatus(anyPath)).thenReturn(status);
        return anyPath;
    }
}

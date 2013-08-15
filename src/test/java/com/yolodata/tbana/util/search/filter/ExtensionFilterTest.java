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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtensionFilterTest {

    @Test
    public void testExtension() throws Exception {
        String csvFilePath = "/path/to/csvFile.csv";

        ExtensionFilter csvFilter = new ExtensionFilter(ExtensionFilter.Extension.CSV);
        ExtensionFilter tsvFilter = new ExtensionFilter(".tsv");

        assertEquals(true,csvFilter.accept(csvFilePath));
        assertEquals(false,tsvFilter.accept(csvFilePath));

    }

    @Test
    public void testExtensionMixedCase() throws Exception {
        String csvFilePath = "/path/to/csvFile.CsV";

        ExtensionFilter csvFilter = new ExtensionFilter(ExtensionFilter.Extension.CSV);

        assertEquals(true,csvFilter.accept(csvFilePath));
    }
}

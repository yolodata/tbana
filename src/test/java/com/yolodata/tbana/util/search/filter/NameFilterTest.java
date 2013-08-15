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

public class NameFilterTest {

    @Test
    public void testFilterWithFileName() throws Exception {
        String csvFilePath = "/path/to/csvFile.csv";

        NameFilter csvFilter = new NameFilter("csvFile.csv");

        assertEquals(true,csvFilter.accept(csvFilePath));
    }


    @Test
    public void testFilterWithWildcard() throws Exception {
        String csvFilePath = "/path/to/csvFile.csv";

        String [] acceptedNames = {"csvFile.*","csv*.csv"};
        NameFilter csvFilter = new NameFilter(acceptedNames);

        assertEquals(true,csvFilter.accept(csvFilePath));
    }

    @Test
    public void testFilterWithMultipleAcceptedNames() throws Exception {
        String acceptedName = "path/to/directory";
        String acceptedName2 = "path/to/accepted";
        String notAccepted = "path/to/something";


        String [] acceptedNames = {"directory","acc*"};
        NameFilter csvFilter = new NameFilter(acceptedNames);

        assertEquals(true,csvFilter.accept(acceptedName));
        assertEquals(true,csvFilter.accept(acceptedName2));
        assertEquals(false,csvFilter.accept(notAccepted));

    }

    @Test
    public void testNameFilterWithEmptyList() throws Exception {
        String acceptedName = "path/to/dir";

        String [] acceptedNames = {};

        NameFilter emptyFilter = new NameFilter(acceptedNames);

        assertEquals(true, emptyFilter.accept(acceptedName));
    }
}

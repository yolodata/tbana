package com.yolodata.tbana.util.search.filter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NameFilterTest {

    @Test
    public void testExtension() throws Exception {
        String csvFilePath = "/path/to/csvFile.csv";

        NameFilter csvFilter = new NameFilter("csvFile.csv");


        assertEquals(true,csvFilter.accept(csvFilePath));

    }
}

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

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
}

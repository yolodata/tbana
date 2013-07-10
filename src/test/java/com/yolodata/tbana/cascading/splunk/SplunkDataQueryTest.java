package com.yolodata.tbana.cascading.splunk;

import org.junit.Test;

public class SplunkDataQueryTest {

    @Test
    public void testDefaultConstructor(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery();
        assert splunkDataQuery.getSplunkQuery().equals("search index=*");
        assert splunkDataQuery.getEarliestTime().equals("0");
        assert splunkDataQuery.getLatestTime().equals("now");
        assert splunkDataQuery.getIndexes().equals("*");
    }

    @Test
    public void testConstructorWithTimeRange(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery("-12h", "now");
        assert splunkDataQuery.getEarliestTime().equals("-12h");
        assert splunkDataQuery.getLatestTime().equals("now");
    }

    @Test
    public void testConstructorWithIndexes(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery("-12h", "now", new String[] {"main", "_internal"});
        assert splunkDataQuery.getSplunkQuery().equals("search index=main OR index=_internal");
        assert splunkDataQuery.getIndexes().equals("main,_internal");
    }
}

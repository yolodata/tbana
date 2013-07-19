package com.yolodata.tbana.hadoop.mapred.splunk;

import org.joda.time.DateTime;
import org.junit.Test;

public class SplunkDataQueryTest {

    @Test
    public void testDefaultConstructor(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery();
        assert splunkDataQuery.getSplunkQuery().equals("search index=*");
        assert splunkDataQuery.getIndexesString().equals("*");
    }

    @Test
    public void testConstructorWithIndexes(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery(new DateTime(0), DateTime.now(), new String[] {"main", "_internal"});
        assert splunkDataQuery.getSplunkQuery().equals("search index=main OR index=_internal");
        assert splunkDataQuery.getIndexesString().equals("main,_internal");
    }
}

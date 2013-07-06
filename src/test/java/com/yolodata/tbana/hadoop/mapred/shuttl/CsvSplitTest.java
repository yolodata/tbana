package com.yolodata.tbana.hadoop.mapred.shuttl;

import com.yolodata.tbana.hadoop.mapred.InputSplitTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class CsvSplitTest extends InputSplitTest {

    @Test
    public void testSerialization() throws Exception {
        CsvSplit split1 = new CsvSplit(new Path("path"),0,1,2,false);
        CsvSplit split2 = new CsvSplit();

        super.testSerialization(split1, split2);
    }
}

package com.yolodata.tbana.hadoop.mapred.shuttl;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.assertEquals;

public class CsvSplitTest {

    @Test
    public void testSerialization() throws Exception {
        CsvSplit split1 = new CsvSplit(new Path("path"),0,1,2,false);
        CsvSplit split2 = new CsvSplit();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(outputStream);

        split1.write(stream);

        byte[] output = outputStream.toByteArray();

        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(output));

        split2.readFields(inputStream);

        assertEquals(split1, split2);
    }
}

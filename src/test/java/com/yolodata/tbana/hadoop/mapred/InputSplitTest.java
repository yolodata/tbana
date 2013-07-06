package com.yolodata.tbana.hadoop.mapred;

import com.yolodata.tbana.hadoop.mapred.shuttl.CsvSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class InputSplitTest {

    protected void testSerialization(InputSplit split, InputSplit emptySplit) throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(outputStream);

        split.write(stream);

        byte[] output = outputStream.toByteArray();

        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(output));

        emptySplit.readFields(inputStream);

        assertEquals(split, emptySplit);

    }
}

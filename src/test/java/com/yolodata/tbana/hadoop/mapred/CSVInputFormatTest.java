package com.yolodata.tbana.hadoop.mapred;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.List;

public class CSVInputFormatTest {
    private FileSystem fs;
    private final String TEST_FOLDER_PATH = "build/test";

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
        fs.mkdirs(new Path(TEST_FOLDER_PATH));
    }

    @After
    public void tearDown() throws Exception {

        // Remove folder with temporary test data
        fs.delete(new Path(TEST_FOLDER_PATH),true);
        fs.close();
    }

    @Test
    public void testJobUsingCSVNLineInputFormat() throws Exception {

        String inputContent = "header1,header2\n" +
                "column1,\"column 2 using\n two lines\"\n" +
                "c1,c2\n";

        Path outputPath = runJob(inputContent);
        assert(outputPath != null); // Means that the job successfully finished

        String outputContent = TestUtils.readMapReduceOutputFile(fs,outputPath);
        assert(inputContent.equals(outputContent));

    }

    private Path runJob(String inputContent) throws Exception {
        Path inputPath = new Path(TEST_FOLDER_PATH.concat("/multilineCSV.in"));
        TestUtils.createFileWithContent(fs,inputPath,inputContent);

        Path outputPath = new Path(TEST_FOLDER_PATH.concat("/multilineCSV.out"));

        if (runJob(inputPath,outputPath) == 0) // successful execution
            return outputPath;

        return null;
    }



    public int runJob(Path inputPath, Path outputPath) throws Exception {
        CSVTestRunner importer = new CSVTestRunner();
        return ToolRunner.run(new Configuration(), importer, new String[]{inputPath.toString(), outputPath.toString()});
    }
}


class TestMapper extends MapReduceBase implements Mapper<LongWritable, List<Text>, LongWritable, Text> {

    @Override
    public void map(LongWritable key, List<Text> values, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        Text output = new Text(StringUtils.join(values, ","));
        outputCollector.collect(null, output);
    }
}

class CSVTestRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
        getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);

        JobConf jobConf = new JobConf(getConf());

        jobConf.setJarByClass(CSVTestRunner.class);
        jobConf.setNumReduceTasks(0);
        jobConf.setMapperClass(TestMapper.class);

        jobConf.setInputFormat(CSVNLineInputFormat.class);
        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);

        CSVNLineInputFormat.setInputPaths(jobConf, new Path(args[0]));
        TextOutputFormat.setOutputPath(jobConf,new Path(args[1]));

        JobClient.runJob(jobConf);

        return 0;
    }

    public CSVTestRunner() {
        super(new Configuration());
    }

}


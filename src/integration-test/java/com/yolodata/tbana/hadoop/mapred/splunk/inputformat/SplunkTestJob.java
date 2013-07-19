package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


class TestMapper extends MapReduceBase implements Mapper<LongWritable, List<Text>, LongWritable, Text> {

    @Override
    public void map(LongWritable key, List<Text> values, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        Text output = new Text(StringUtils.join(values, ","));
        outputCollector.collect(key, output);
    }
}

class TestReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    public void reduce(LongWritable longWritable, Iterator<Text> textIterator, OutputCollector<LongWritable, Text> longWritableTextOutputCollector, Reporter reporter) throws IOException {
        while(textIterator.hasNext())
            longWritableTextOutputCollector.collect(longWritable,textIterator.next());
    }
}

class SplunkTestRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf());

        jobConf.set(SplunkInputFormat.INPUTFORMAT_MODE,args[0]);
        jobConf.setJarByClass(SplunkTestRunner.class);
        jobConf.setNumReduceTasks(1);
        jobConf.setMapperClass(TestMapper.class);
        jobConf.setReducerClass(TestReducer.class);

        jobConf.setInputFormat(SplunkInputFormat.class);
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(jobConf ,new Path(args[1]));

        JobClient.runJob(jobConf);

        return 0;
    }

    public SplunkTestRunner(Configuration conf) {
        super(conf);
    }
}
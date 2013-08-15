/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.hadoop.mapred.csv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>>{

	@Override
	public RecordReader<LongWritable, List<Text>> getRecordReader(
			InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
        if(jobConf.get(CSVLineRecordReader.FORMAT_DELIMITER) == null ||
				jobConf.get(CSVLineRecordReader.FORMAT_SEPARATOR) == null) {
			throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
		}
		
		CSVLineRecordReader reader = new CSVLineRecordReader();
        reader.initialize(split, jobConf);
		return reader;
		
	}
}

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

package com.yolodata.tbana.hadoop.mapred.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * This class is used to allow serialization and deserialization from hadoop
 * side. Besides that, it behaves as a simple ArrayList
 * 
 * @author mvallebr
 *
 *
 * Source: https://github.com/mvallebr/CSVInputFormat
 */
public class ArrayListTextWritable extends ArrayList<Text> implements Writable {
	private static final long serialVersionUID = -6737762624115237320L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput dataoutput) throws IOException {
		dataoutput.writeInt(this.size());
		for (Text element : this) {
			element.write(dataoutput);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput datainput) throws IOException {
		this.clear();
		int count = datainput.readInt();
		for (int i = 0; i < count; i++) {
			try {
				Text obj = Text.class.newInstance();
				obj.readFields(datainput);
				this.add(obj);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

}

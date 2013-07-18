package com.yolodata.tbana.hadoop.mapred.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
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
public class ArrayListTextWritable extends ArrayList<TextSerializable> implements Writable,Serializable {
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
				TextSerializable obj = TextSerializable.class.newInstance();
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

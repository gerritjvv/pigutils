package org.nts.sparkhdfs.proto2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LZOProtoInputFormat extends
		LzoInputFormat<Writable, BytesWritable> {

	@Override
	public RecordReader<Writable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext ctx) throws IOException,
			InterruptedException {
		return new LZOProtoReader();
	}

}

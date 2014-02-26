package org.nts.pigutils.proto;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * Reads a file that contains lzo messages in the format [4 bytes message length ] [gpb message bytes]
 * 
 */
public class LzoRecordReader extends RecordReader<IntWritable, BytesWritable>{

	private Path splitPath;
	private FSDataInputStream in;

	private IntWritable currentKey;
	private BytesWritable currentValue;
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		final FileSplit fileSplit = (FileSplit) genericSplit;
		splitPath = fileSplit.getPath();
		FileSystem fs = splitPath.getFileSystem(context.getConfiguration());
		
		in = fs.open(splitPath);
		
	}

	public Path getSplitPath(){
		return splitPath;
	}

	@Override
	public void close() throws IOException {
		
		in.close();
		
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return in.getPos();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
	   if(in.available() > 0){
		
		final int len = in.readInt();
		final byte[] bts = new byte[len];
		in.read(bts);
		
		currentValue = new BytesWritable(bts);
		currentKey = new IntWritable(bts.hashCode());
		
		return true;
	   }else{
		return false;
	   }
	}
	
}

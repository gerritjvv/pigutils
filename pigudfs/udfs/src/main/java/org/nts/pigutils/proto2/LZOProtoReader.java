package org.nts.pigutils.proto2;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 
 * 
 */
public class LZOProtoReader extends RecordReader<Writable, BytesWritable> {

	DataInputStream in;
	IntWritable key = new IntWritable();
	byte[] bts = new byte[1024];

	BytesWritable value = new BytesWritable(bts);

	int records = 0;
	FSDataInputStream path_input;
	Path splitPath;

	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
	}

	@Override
	public Writable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return path_input.available() / path_input.getPos();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctx)
			throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) split;

		Path path = fileSplit.getPath();
		this.splitPath = path;

		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
				ctx.getConfiguration());
		final CompressionCodec codec = compressionCodecs.getCodec(path);

		if (codec == null) {
			throw new IOException("No codec for file " + path
					+ " not found, cannot run");
		}

		FileSystem fs = FileSystem.get(ctx.getConfiguration());

		path_input = fs.open(path);

		in = new DataInputStream(new BufferedInputStream(
				codec.createInputStream(path_input)));
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			// this method will do: readInt, read bytes
			value.readFields(in);

			key.set(records++);

			return true;
		} catch (EOFException eof) {
			return false;
		}
	}

	public Path getSplitPath() {
		return splitPath;
	}

}

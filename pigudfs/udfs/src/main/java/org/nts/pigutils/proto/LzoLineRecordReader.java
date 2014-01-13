package org.nts.pigutils.proto;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LzoLineRecordReader extends com.hadoop.mapreduce.LzoLineRecordReader{

	private Path splitPath;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		final FileSplit fileSplit = (FileSplit) genericSplit;
		splitPath = fileSplit.getPath();
		super.initialize(genericSplit, context);
	}

	public Path getSplitPath(){
		return splitPath;
	}
	
}

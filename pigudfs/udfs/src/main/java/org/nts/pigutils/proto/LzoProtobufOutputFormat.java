package org.nts.pigutils.proto;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.data.Tuple;

/**
 * LzoOutputFormat that returns the LzoProtobufB64LineRecordWriter
 * 
 */
public class LzoProtobufOutputFormat extends
		FileOutputFormat<Writable, Tuple> {

	String clsMapping = null;

	public LzoProtobufOutputFormat(String clsMapping) {
		this.clsMapping = clsMapping;
	}

	@Override
	public RecordWriter<Writable, Tuple> getRecordWriter(TaskAttemptContext ctx)
			throws IOException, InterruptedException {

		Path path = getDefaultWorkFile(ctx, ".lzo");

		LzoProtobufRecordWriter writer = new LzoProtobufRecordWriter(
				ProtobufClassUtil.loadProtoClass(clsMapping,
						ctx.getConfiguration()));
		writer.init(ctx, path);

		return writer;
	}

}


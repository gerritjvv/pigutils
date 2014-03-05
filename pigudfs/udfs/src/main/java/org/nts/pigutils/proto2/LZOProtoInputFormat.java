package org.nts.pigutils.proto2;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadFunc;
import org.nts.pigutils.proto.LzoInputFormat;
import org.nts.pigutils.store.util.PathPartitionHelper;

public class LZOProtoInputFormat extends
		LzoInputFormat<Writable, BytesWritable> {

	transient PathPartitionHelper partitionHelper = new PathPartitionHelper();
	final Class<? extends LoadFunc> loaderClass;
	final String signature;

	public LZOProtoInputFormat(Class<? extends LoadFunc> loaderClass,
			String signature) {
		super();
		this.loaderClass = loaderClass;
		this.signature = signature;
	}

	@Override
	public RecordReader<Writable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext ctx) throws IOException,
			InterruptedException {
		return new LZOProtoReader();
	}

	@Override
	protected List<FileStatus> listStatus(final JobContext ctx)
			throws IOException {

		Collection<FileStatus> files = null;

		try {
			files = partitionHelper.listStatus(ctx, loaderClass, signature,
					new FilenameFilter() {

						public boolean accept(File dir, String name) {
							return name.endsWith(".lzo");
						}

					});
		} catch (InterruptedException e) {
			Thread.interrupted();
			return null;
		} catch (ExecutionException excp) {
			throw new RuntimeException(excp);
		}

		if (files == null) {
			files = super.listStatus(ctx);
		} else {

			System.out.println("Listing Indexes");
		}

		System.out.println("Found " + files.size() + " files");
		return new ArrayList<FileStatus>(files);

	}

}

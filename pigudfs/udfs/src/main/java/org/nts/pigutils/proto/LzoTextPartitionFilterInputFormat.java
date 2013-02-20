package org.nts.pigutils.proto;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadFunc;
import org.nts.pigutils.store.util.PathPartitionHelper;


/**
 * Is a wrapper arround other input formats giving them partition filter
 * capabilities.
 * 
 * @param <K>
 * @param <V>
 */
public class LzoTextPartitionFilterInputFormat extends LzoInputFormat {

	transient PathPartitionHelper partitionHelper = new PathPartitionHelper();
	Class<? extends LoadFunc> loaderClass;
	String signature;

	public LzoTextPartitionFilterInputFormat(
			Class<? extends LoadFunc> loaderClass, String signature) {
		super();
		this.loaderClass = loaderClass;
		this.signature = signature;
	}

	
	@Override
	public List<InputSplit> getSplits(JobContext ctx) throws IOException {
		return super.getSplits(ctx);
	}


	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return super.isSplitable(context, filename);
	}


	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext taskAttempt) {

		return new LzoLineRecordReader();
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

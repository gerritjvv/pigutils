package org.nts.pigutils.lucene;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LuceneOutputFormat extends FileOutputFormat<Writable, Document> {

	String location;
	FileSystem fs;
	String taskid;

	FileOutputCommitter committer;
	AtomicInteger counter = new AtomicInteger();

	public LuceneOutputFormat(String location) {
		this.location = location;
	}
	
	@Override
	public RecordWriter<Writable, Document> getRecordWriter(
			TaskAttemptContext ctx) throws IOException, InterruptedException {

		Configuration conf = ctx.getConfiguration();
		fs = FileSystem.get(conf);

		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		String baseName = System.currentTimeMillis() + "-";
		File tempDir = new File(baseDir, baseName + counter.getAndIncrement());
		tempDir.mkdirs();
		tempDir.deleteOnExit();

		return new LuceneRecordWriter(
				(FileOutputCommitter) getOutputCommitter(ctx), tempDir);
	}

	/**
	 * Write out the LuceneIndex to a local temporary location.<br/>
	 * On commit/close the index is copied to the hdfs output directory.<br/>
	 *
	 */
	static class LuceneRecordWriter extends RecordWriter<Writable, Document> {

		final IndexWriter writer;
		final FileOutputCommitter committer;
		final File tmpdir;

		public LuceneRecordWriter(FileOutputCommitter committer, File tmpdir) {
			try {
				this.committer = committer;
				this.tmpdir = tmpdir;
				IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42,
						new StandardAnalyzer(Version.LUCENE_42));
				LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
			    mergePolicy.setMergeFactor(10);
			    mergePolicy.setUseCompoundFile(false);
			    config.setMergePolicy(mergePolicy);
			    config.setMergeScheduler(new SerialMergeScheduler());

				writer = new IndexWriter(FSDirectory.open(tmpdir),
						config);
				
			} catch (IOException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

		@Override
		public void close(final TaskAttemptContext ctx) throws IOException,
				InterruptedException {
			//use a thread for status polling
			final Thread th = new Thread() {
				public void run() {
					ctx.progress();
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}
				}
			};
			th.start();
			try {
				writer.forceMerge(1);
				writer.close();
				// move all files to part
				Configuration conf = ctx.getConfiguration();

				Path work = committer.getWorkPath();
				Path output = new Path(work, "index-"
						+ ctx.getTaskAttemptID().getTaskID().getId());
				FileSystem fs = FileSystem.get(conf);

				FileUtil.copy(tmpdir, fs, output, true, conf);
			} finally {
				th.interrupt();
			}
		}

		@Override
		public void write(Writable key, Document doc) throws IOException,
				InterruptedException {
			writer.addDocument(doc);

		}

	}
}

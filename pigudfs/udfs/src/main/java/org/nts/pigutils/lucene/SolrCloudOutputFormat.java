package org.nts.pigutils.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrCloudOutputFormat extends
		FileOutputFormat<Writable, SolrInputDocument> {

	final String address;
	final String collection;

	public SolrCloudOutputFormat(String address, String collection) {
		this.address = address;
		this.collection = collection;
	}

	@Override
	public RecordWriter<Writable, SolrInputDocument> getRecordWriter(
			TaskAttemptContext ctx) throws IOException, InterruptedException {
		return new SolrCloudRecordWriter(ctx, address, collection);
	}

	
	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext arg0) throws IOException {
		return new OutputCommitter(){

			@Override
			public void abortTask(TaskAttemptContext ctx) throws IOException {
				
			}

			@Override
			public void commitTask(TaskAttemptContext ctx) throws IOException {
				
			}

			@Override
			public boolean needsTaskCommit(TaskAttemptContext arg0)
					throws IOException {
				return true;
			}

			@Override
			public void setupJob(JobContext ctx) throws IOException {
				
			}

			@Override
			public void setupTask(TaskAttemptContext ctx) throws IOException {
				
			}
			
			
		};
	}


	/**
	 * Write out the LuceneIndex to a local temporary location.<br/>
	 * On commit/close the index is copied to the hdfs output directory.<br/>
	 * 
	 */
	static class SolrCloudRecordWriter extends
			RecordWriter<Writable, SolrInputDocument> {

		CloudSolrServer server;

		int batch = 1000;
		
		TaskAttemptContext ctx;
		
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(batch);
		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		/**
		 * Opens and forces connect to CloudSolrServer
		 * 
		 * @param address
		 */
		public SolrCloudRecordWriter(final TaskAttemptContext ctx, String address, String collection) {
			try {
				this.ctx = ctx;
				server = new CloudSolrServer(address);
				server.setDefaultCollection(collection);
				server.connect();
				
				exec.scheduleWithFixedDelay(new Runnable(){
					public void run(){
						ctx.progress();
					}
				}, 1000, 1000, TimeUnit.MILLISECONDS);
			} catch (IOException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

		
		/**
		 * On close we commit
		 */
		@Override
		public void close(final TaskAttemptContext ctx) throws IOException,
				InterruptedException {

			try {
				
				if (docs.size() > 0) {
					server.add(docs);
					docs.clear();
				}

				server.commit();
			} catch (SolrServerException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			} finally {
				server.shutdown();
				exec.shutdownNow();
			}
			
		}

		/**
		 * We add the indexed documents without commit
		 */
		@Override
		public void write(Writable key, SolrInputDocument doc)
				throws IOException, InterruptedException {
			try {
				docs.add(doc);
				if (docs.size() >= batch) {
					server.add(docs);
					docs.clear();
				}
			} catch (SolrServerException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

	}
}

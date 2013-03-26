package org.nts.pigutils.lucene;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrCloudOutputFormat extends
		FileOutputFormat<Writable, SolrInputDocument> {

	String address;

	public SolrCloudOutputFormat(String address) {
		this.address = address;
	}

	@Override
	public RecordWriter<Writable, SolrInputDocument> getRecordWriter(
			TaskAttemptContext ctx) throws IOException, InterruptedException {
		return new SolrCloudRecordWriter(address);
	}

	/**
	 * Write out the LuceneIndex to a local temporary location.<br/>
	 * On commit/close the index is copied to the hdfs output directory.<br/>
	 * 
	 */
	static class SolrCloudRecordWriter extends
			RecordWriter<Writable, SolrInputDocument> {

		CloudSolrServer server;

		/**
		 * Opens and forces connect to CloudSolrServer
		 * @param address
		 */
		public SolrCloudRecordWriter(String address) {
			try {
				server = new CloudSolrServer(address);
				server.connect();
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
				server.commit();
			} catch (SolrServerException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}

			server.shutdown();
		}

		/**
		 * We add the indexed documents without commit
		 */
		@Override
		public void write(Writable key, SolrInputDocument doc)
				throws IOException, InterruptedException {
			try {
				server.add(doc);
			} catch (SolrServerException e) {
				RuntimeException exc = new RuntimeException(e.toString(), e);
				exc.setStackTrace(e.getStackTrace());
				throw exc;
			}
		}

	}
}

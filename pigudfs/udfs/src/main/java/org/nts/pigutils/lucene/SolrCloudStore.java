package org.nts.pigutils.lucene;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.solr.common.SolrInputDocument;

/**
 * 
 * Create a lucene index
 * 
 */
public class SolrCloudStore extends StoreFunc implements StoreMetadata {

	private static final String SCHEMA_SIGNATURE = "solr.output.schema";

	ResourceSchema schema;
	String udfSignature;
	RecordWriter<Writable, SolrInputDocument> writer;

	String address;
	String collection;
	
	public SolrCloudStore(String address, String collection) {
		this.address= address;
		this.collection = collection;
	}

	public void storeStatistics(ResourceStatistics stats, String location,
			Job job) throws IOException {
	}

	public void storeSchema(ResourceSchema schema, String location, Job job)
			throws IOException {
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p = udfc.getUDFProperties(this.getClass(),
				new String[] { udfSignature });
		p.setProperty(SCHEMA_SIGNATURE, s.toString());
	}

	public OutputFormat<Writable, SolrInputDocument> getOutputFormat()
			throws IOException {
		// not be used
		return new SolrCloudOutputFormat(address, collection);
	}

	/**
	 * Not used
	 */
	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		this.udfSignature = signature;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
		UDFContext udc = UDFContext.getUDFContext();
		String schemaStr = udc.getUDFProperties(this.getClass(),
				new String[] { udfSignature }).getProperty(SCHEMA_SIGNATURE);

		if (schemaStr == null) {
			throw new RuntimeException("Could not find udf signature");
		}

		schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));

	}

	@Override
	public void putNext(Tuple t) throws IOException {

		final SolrInputDocument doc = new SolrInputDocument();
		
		final ResourceFieldSchema[] fields = schema.getFields();
		int docfields = 0;

		for (int i = 0; i < fields.length; i++) {
			final Object value = t.get(i);

			if (value != null) {
				docfields++;
				doc.addField(fields[i].getName().trim(), value.toString());
			}
			
		}

		try {
			if (docfields > 0)
				writer.write(null, doc);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

	}

}

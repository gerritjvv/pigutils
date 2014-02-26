package org.nts.pigutils.proto;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.counters.PigCounterHelper;
import org.nts.pigutils.store.util.PathPartitionHelper;

import com.google.protobuf.Message;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * 
 * Simple store function that uses the LzoProtobufOutputFormat to write Protobuf
 * base64 lzo line output.<br/>
 * 
 * The class has a required string parameter. <br/>
 * This is used as a friendly property mapping to ProtoBuff class and abstracts
 * the pig scripts from having to contain the actual protobuf class names.
 * <p/>
 * e.g.<br/>
 * <code>
 * a = LOAD '$INPUT' using com.twitter.elephantbird.pig.store.LzoProtobuffStore('person');
 * </code> <br/>
 * The above code will look for a properties declared person.<br/>
 * If we have this in the script itself or better in the
 * $PIG_HOME/conf/pig.properties like so:<br/>
 * person=MyProtoClass<br/>
 * Then the this loader will get the MyProtoClass from the configuration and use
 * it to write all Tuples.
 * 
 */
public class LzoProtobuffPigStore extends PigStorage implements LoadMetadata {

	enum FORMAT {
		BADGPB, GENERAL_GPB_ERROR
	};

	String clsMapping;

	private final ProtobufToPig protoToPig = new ProtobufToPig();

	private int[] requiredIndices = null;
	private int[] requiredPartitionIndices = null;

	private boolean requiredIndicesInitialized = false;

	PigCounterHelper counterHelper = new PigCounterHelper();

	private String signature;

	Method newBuilder;

	/**
	 * Implements the logic for searching partition keys and applying partition
	 * filtering
	 */
	transient PathPartitionHelper pathPartitionerHelper = new PathPartitionHelper();

	private LzoRecordReader reader;

	transient Set<String> partitionColumns = null;
	/**
	 * Length of schema tuple
	 */
	transient int schemaLength = 0;

	transient private String[] partitionKeys = null;
	transient private Map<String, String> partitionValues = null;

	transient private Path currentPath = null;

	protected enum LzoProtobuffPigStoreCounts {
		LinesRead, ProtobufsRead
	}

	public LzoProtobuffPigStore(String clsMapping) {
		this.clsMapping = clsMapping;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() {
		return new LzoProtobufOutputFormat(clsMapping);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new LzoPartitionFilterInputFormat(getClass(), signature);
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		super.setStoreLocation(location, job);

		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		FileOutputFormat.setOutputPath(job, new Path(location));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	}

	/**
	 * Creates the requiredIndices int array from the UDFContext.<br/>
	 * This action is only performed once, subsequent method calls will only
	 * return.
	 * 
	 * @throws IOException
	 */
	private void checkRequiredColumnsInit() throws IOException {
		if (!requiredIndicesInitialized) {
			requiredIndicesInitialized = true;
			String value = UDFContext.getUDFContext()
					.getUDFProperties(this.getClass()).getProperty(signature);

			int requiredIndicesAll[] = null;
			Set<Integer> requiredIndicesSet = new HashSet<Integer>();
			Set<Integer> keysIndicesSet = new HashSet<Integer>();

			int len = schemaLength - partitionKeys.length;

			if (value != null) {
				requiredIndicesAll = (int[]) ObjectSerializer
						.deserialize(value);
				Arrays.sort(requiredIndicesAll);
				for (int indice : requiredIndicesAll) {

					if (indice < len) {
						requiredIndicesSet.add(new Integer(indice));
					} else {
						System.out.println("Adding index from pig: " + indice
								+ " translate to " + (indice - len));
						keysIndicesSet.add(new Integer(indice - len));
					}

				}
			} else {
				for (int i = 0; i < len; i++) {
					requiredIndicesSet.add(new Integer(i));
				}
				for (int i = 0; i < partitionKeys.length; i++) {
					keysIndicesSet.add(new Integer(i));
				}
			}

			requiredIndices = new int[requiredIndicesSet.size()];
			requiredPartitionIndices = new int[keysIndicesSet.size()];

			int index = 0;
			for (Integer i : requiredIndicesSet)
				requiredIndices[index++] = i.intValue();
			index = 0;
			for (Integer i : keysIndicesSet)
				requiredPartitionIndices[index++] = i.intValue();

			Arrays.sort(requiredIndices);
			Arrays.sort(requiredPartitionIndices);

		}
	}

	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;

		try {
			// check that the required columns indices have been read if any
			checkRequiredColumnsInit();

			boolean printedError = false;

			// read while true
			// we only break if we can read a correct value
			while (reader.nextKeyValue()) {

				BytesWritable value = reader.getCurrentValue();
				if (value.getLength() > 0) {

					try {
						Message.Builder builder = (Message.Builder) newBuilder
								.invoke(null, new Object[] {});
						Message protoValue = builder
								.mergeFrom(value.getBytes()).build();
						
						if (protoValue != null) {

							if (requiredIndices.length > 0) {
								tuple = new ProtobufTuple(protoValue,
										requiredIndices);
							} else {
								tuple = TupleFactory.getInstance().newTuple();
							}

							Path path = reader.getSplitPath();

							if (currentPath == null
									|| !currentPath.equals(path)) {
								partitionValues = (partitionKeys == null) ? null
										: pathPartitionerHelper
												.getPathPartitionKeyValues(path
														.toString());
								currentPath = path;
							}

							if (requiredPartitionIndices.length > 0) {

								for (int indice : requiredPartitionIndices) {
									tuple.append(partitionValues
											.get(partitionKeys[indice]));
								}

							}

							break;
						} else {
							incrCounter(FORMAT.BADGPB, 1l);
						}

					} catch (Throwable t) {
						if (!printedError) {
							t.printStackTrace();
							printedError = true;

						}
						incrCounter(FORMAT.BADGPB, 1L);
					}

				} else {
					incrCounter(FORMAT.GENERAL_GPB_ERROR, 1l);
				}

			}

		} catch (Exception e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

		return tuple;
	}

	@SuppressWarnings("rawtypes")
	protected void incrCounter(Enum key, long incr) {
		counterHelper.incrCounter(key, incr);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		super.prepareToRead(reader, split);

		Class<? extends com.google.protobuf.Message> cls = (Class<? extends Message>) ProtobufClassUtil
				.loadProtoClass(clsMapping, split.getConf());

		try {
			newBuilder = cls.getMethod("newBuilder", new Class[] {});
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}

		// save schema length
		Schema schema = protoToPig.toSchema(Protobufs
				.getMessageDescriptor(ProtobufClassUtil.loadProtoClass(
						clsMapping, split.getConf())));

		try {
			partitionKeys = getPartitionKeys(null, null);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		schemaLength = schema.size() + partitionKeys.length;

		this.reader = (LzoRecordReader) reader;
	}

	@Override
	public ResourceSchema getSchema(String filename, Job job)
			throws IOException {

		Set<String> keys = getPartitionColumns(filename, job);

		Schema schema = null;
		try {
			schema = protoToPig.toSchema(Protobufs
					.getMessageDescriptor(ProtobufClassUtil.loadProtoClass(
							clsMapping, job.getConfiguration())));

		} finally {
		}
		if (keys != null && keys.size() > 0) {
			for (String key : keys) {
				// only add a partition key if the schema does no already
				// contain a value of it.
				if (schema.getField(key) == null) {
					schema.add(new FieldSchema(key, DataType.CHARARRAY));
				}
			}
		}

		return new ResourceSchema(schema);
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		RequiredFieldResponse response = null;

		if (!(requiredFieldList == null || requiredFieldList.getFields() == null)) {

			// convert the list of RequiredFieldList objects into an array of
			// int
			// each item in the array containns the required field index.
			// Note that this array of int is sorted.
			List<RequiredField> requiredFields = requiredFieldList.getFields();

			int requiredIndices[] = new int[requiredFields.size()];

			for (int i = 0; i < requiredFields.size(); i++) {
				RequiredField field = requiredFields.get(i);
				if (field.getSubFields() != null) {
					System.out.println("Subfields are not supported: "
							+ Arrays.toString(field.getSubFields().toArray()));

				}
				requiredIndices[i] = requiredFields.get(i).getIndex();
			}

			// we must sort this array. The logic that reads from it required
			// this.
			// this is a map between the required Index and the real index
			// e.g. [0] => maps to [3]
			Arrays.sort(requiredIndices);

			System.out.println("Setting required indices: "
					+ Arrays.toString(requiredIndices));
			try {
				UDFContext
						.getUDFContext()
						.getUDFProperties(this.getClass())
						.setProperty(signature,
								ObjectSerializer.serialize(requiredIndices));
			} catch (Exception e) {
				throw new RuntimeException("Cannot serialize requiredIndices");
			}

			response = new RequiredFieldResponse(true);
		}

		return response;
	}

	@Override
	public void setUDFContextSignature(String signature) {
		this.signature = signature;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		Set<String> partitionKeys = getPartitionColumns(location, job);

		return partitionKeys == null ? new String[0] : partitionKeys
				.toArray(new String[] {});
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
		getUDFContext().setProperty(
				PathPartitionHelper.PARITITION_FILTER_EXPRESSION,
				partitionFilter.toString());
	}

	/**
	 * Reads the partition columns
	 * 
	 * @param location
	 * @param job
	 * @return
	 */
	private Set<String> getPartitionColumns(String loadInputPath, Job job) {

		if (partitionColumns == null) {

			// read the partition columns from the UDF Context first.
			// if not in the UDF context then read it using the PathPartitioner.

			Properties properties = getUDFContext();

			if (properties == null)
				properties = new Properties();

			String partitionColumnStr = properties
					.getProperty(PathPartitionHelper.PARTITION_COLUMNS);
			System.out.println("partitionColumnStr " + partitionColumnStr);

			if (partitionColumnStr == null
					&& !(loadInputPath == null || job == null)) {
				// if it hasn't been written yet.

				String loadInputPathSplit[] = loadInputPath.split("[,; ]");
				if (loadInputPathSplit.length < 1) {
					throw new RuntimeException("The input path "
							+ loadInputPath + " cannot be empty");
				}
				String location = loadInputPathSplit[0];

				Set<String> partitionColumnSet;
				try {
					partitionColumnSet = pathPartitionerHelper
							.getPartitionKeys(location, job.getConfiguration());

					System.out.println("ParitionColumnSet: "
							+ Arrays.toString(partitionColumnSet.toArray()));
				} catch (IOException e) {

					RuntimeException rte = new RuntimeException(e);
					rte.setStackTrace(e.getStackTrace());
					throw rte;

				}

				if (partitionColumnSet != null) {

					StringBuilder buff = new StringBuilder();

					int i = 0;
					for (String column : partitionColumnSet) {
						if (i++ != 0) {
							buff.append(',');
						}

						buff.append(column);
					}

					String buffStr = buff.toString().trim();

					if (buffStr.length() > 0) {

						properties.setProperty(
								PathPartitionHelper.PARTITION_COLUMNS,
								buff.toString());
					}

					partitionColumns = partitionColumnSet;

				}

			} else {
				// the partition columns has been set already in the UDF Context
				if (partitionColumnStr != null) {
					String split[] = partitionColumnStr.split(",");
					partitionColumns = new LinkedHashSet<String>();
					if (split.length > 0) {
						for (String splitItem : split) {
							partitionColumns.add(splitItem);
						}
					}
				}

			}

		}

		return partitionColumns;

	}

	private Properties getUDFContext() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] { signature });
	}

}

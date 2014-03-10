package org.nts.pigutils.proto2;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.counters.PigCounterHelper;
import org.nts.pigutils.proto.ProtobufClassUtil;
import org.nts.pigutils.proto.ProtobufToPig;
import org.nts.pigutils.proto.ProtobufTuple;
import org.nts.pigutils.proto.Protobufs;
import org.nts.pigutils.store.util.PathPartitionHelper;

import com.google.protobuf.Message;

/**
 * 
 * Pig storage class to read data stored without base64 and as <br/>
 * len bts len bts ... <br/>
 * 
 */
public class LZOProtoStore extends PigStorage {

	private final ProtobufToPig protoToPig = new ProtobufToPig();

	LZOProtoReader reader;
	TupleFactory factory = TupleFactory.getInstance();
	private String loadLocation;
	private String clsMapping;
	private Method newBuilder;

	private int[] requiredIndices = null;
	private int[] requiredPartitionIndices = null;

	private boolean requiredIndicesInitialized = false;

	transient private String[] partitionKeys = null;
	transient Set<String> partitionColumns = null;
	/**
	 * Length of schema tuple
	 */
	transient int schemaLength = 0;

	transient private Map<String, String> partitionValues = null;

	transient private Path currentPath = null;

	PigCounterHelper counterHelper = new PigCounterHelper();

	/**
	 * Implements the logic for searching partition keys and applying partition
	 * filtering
	 */
	transient PathPartitionHelper pathPartitionerHelper = new PathPartitionHelper();

	enum FORMAT {
		BADGPB, GENERAL_GPB_ERROR
	};

	protected enum LzoProtobuffPigStoreCounts {
		LinesRead, ProtobufsRead
	}

	public LZOProtoStore() {
	}

	public LZOProtoStore(String clsMapping) {
		this.clsMapping = clsMapping;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new LZOProtoInputFormat(getClass(), signature);
	}
	
	

	@Override
	public ResourceSchema getSchema(String filename, Job job) throws IOException {
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
	public void prepareToRead(
			@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
		this.reader = (LZOProtoReader) reader;
		
		Class<? extends com.google.protobuf.Message> cls = (Class<? extends
		 Message>) ProtobufClassUtil .loadProtoClass(clsMapping,
		 split.getConf());
		 
		 try { newBuilder = cls.getMethod("newBuilder", new Class[] {}); }
		 catch (Throwable t) { throw new RuntimeException(t); }
		 
		 // save schema length 
		 Schema schema = protoToPig.toSchema(Protobufs
		 .getMessageDescriptor(ProtobufClassUtil.loadProtoClass( clsMapping,
		 split.getConf())));
		 
		 try { partitionKeys = getPartitionKeys(null, null);
		 
		 } catch (IOException e) { throw new RuntimeException(e); }
		 
		 schemaLength = schema.size() + partitionKeys.length;
		
	}

	public void setLocation(String location, Job job) throws IOException {
		loadLocation = location;
		System.out.println("Load location: " + loadLocation);
		FileInputFormat.setInputPaths(job, location);
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
						
						//System.out.println("str: " + new String(value.getBytes()));
						Message.Builder builder = (Message.Builder) newBuilder
								.invoke(null, new Object[] {});
						Message protoValue = builder.mergeFrom(value.getBytes(), 0, value.getLength()).build();
						
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
	
	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		Set<String> partitionKeys = getPartitionColumns(location, job);

		return partitionKeys == null ? new String[0] : partitionKeys
				.toArray(new String[] {});
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

	@Override
	public void setUDFContextSignature(String signature) {
		this.signature = signature;
	}
	
	private Properties getUDFContext() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] { signature });
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

	@SuppressWarnings("rawtypes")
	protected void incrCounter(Enum key, long incr) {
		counterHelper.incrCounter(key, incr);
	}
	
}

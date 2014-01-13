package org.nts.pigutils.store.util;

import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.el.ELContext;
import javax.el.ELResolver;
import javax.el.ExpressionFactory;
import javax.el.FunctionMapper;
import javax.el.ValueExpression;
import javax.el.VariableMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.util.UDFContext;

/**
 * Implements the logic for:<br/>
 * <ul>
 * <li>Listing partition keys and values used in an hdfs path</li>
 * <li>Filtering of partitions from a pig filter operator expression</li>
 * </ul>
 * <p/>
 * <b>Restrictions</b> <br/>
 * Function calls are not supported by this partition helper and it can only
 * handle String values.<br/>
 * This is normally not a problem given that partition values are part of the
 * hdfs folder path and is given a<br/>
 * determined value that would not need parsing by any external processes.<br/>
 * 
 * 
 */
public class PathPartitionHelper {

	public static final String PARTITION_COLUMNS = PathPartitionHelper.class
			+ ".partition-columns";
	public static final String PARITITION_FILTER_EXPRESSION = PathPartitionHelper.class
			.getName() + ".partition-filter";

	private static final Logger LOG = Logger
			.getLogger(PathPartitionHelper.class);

	transient PathPartitioner pathPartitioner = new PathPartitioner();

	/**
	 * Returns the Partition keys and each key's value for a single location.<br/>
	 * That is the location must be something like
	 * mytable/partition1=a/partition2=b/myfile.<br/>
	 * This method will return a map with [partition1='a', partition2='b']<br/>
	 * The work is delegated to the PathPartitioner class
	 * 
	 * @param location
	 * @return Map of String, String
	 * @throws IOException
	 */
	public Map<String, String> getPathPartitionKeyValues(String location)
			throws IOException {
		return pathPartitioner.getPathPartitionKeyValues(location);
	}

	/**
	 * Returns the partition keys for a location.<br/>
	 * The work is delegated to the PathPartitioner class
	 * 
	 * @param location
	 *            String must be the base directory for the partitions
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public Set<String> getPartitionKeys(String location, Configuration conf)
			throws IOException {
		return pathPartitioner.getPartitionKeys(location, conf);
	}

	/**
	 * Sets the PARITITION_FILTER_EXPRESSION property in the UDFContext
	 * identified by the loaderClass.
	 * 
	 * @param partitionFilterExpression
	 * @param loaderClass
	 * @throws IOException
	 */
	public void setPartitionFilterExpression(String partitionFilterExpression,
			Class<? extends LoadFunc> loaderClass, String signature)
			throws IOException {

		UDFContext
				.getUDFContext()
				.getUDFProperties(loaderClass, new String[] { signature })
				.setProperty(PARITITION_FILTER_EXPRESSION,
						partitionFilterExpression);

	}

	/**
	 * Reads the partition keys from the location i.e the base directory
	 * 
	 * @param location
	 *            String must be the base directory for the partitions
	 * @param conf
	 * @param loaderClass
	 * @throws IOException
	 */
	public void setPartitionKeys(String location, Configuration conf,
			Class<? extends LoadFunc> loaderClass, String signature)
			throws IOException {

		Set<String> partitionKeys = getPartitionKeys(location, conf);

		if (partitionKeys != null) {
			StringBuilder buff = new StringBuilder();
			int i = 0;
			for (String key : partitionKeys) {
				if (i++ != 0) {
					buff.append(",");
				}

				buff.append(key);
			}

			UDFContext.getUDFContext()
					.getUDFProperties(loaderClass, new String[] { signature })
					.setProperty(PARTITION_COLUMNS, buff.toString());
		}

	}

	/**
	 * This method is called by the FileInputFormat to find the input paths for
	 * which splits should be calculated.<br/>
	 * If applyDateRanges == true: Then the HiveRCDateSplitter is used to apply
	 * filtering on the input files.<br/>
	 * Else the default FileInputFormat listStatus method is used.
	 * 
	 * @param ctx
	 *            JobContext
	 * @param loaderClass
	 *            this is chosen to be a subclass of LoadFunc to maintain some
	 *            consistency.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Collection<FileStatus> listStatus(JobContext ctx,
			Class<? extends LoadFunc> loaderClass, String signature,
			FilenameFilter filter) throws IOException, InterruptedException,
			ExecutionException {

		Properties properties = UDFContext.getUDFContext().getUDFProperties(
				loaderClass, new String[] { signature });

		final String partitionExpression = properties
				.getProperty(PARITITION_FILTER_EXPRESSION);

		final ExpressionFactory expressionFactory;

		if (partitionExpression != null) {
			expressionFactory = ExpressionFactory.newInstance();
		} else {
			expressionFactory = null;
		}

		String partitionColumnStr = properties
				.getProperty(PathPartitionHelper.PARTITION_COLUMNS);
		final String[] partitionKeys = (partitionColumnStr == null) ? null
				: partitionColumnStr.split(",");

		Path[] inputPaths = FileInputFormat.getInputPaths(ctx);

		final Collection<FileStatus> splitPaths = new ConcurrentLinkedQueue<FileStatus>();

		final ExecutorService service = Executors.newCachedThreadPool();
		TreeCallable<String> callable = new TreeCallable<String>(null);

		if (partitionKeys != null) {

			for (Path inputPath : inputPaths) {
				// for each input path work recursively through each partition
				// level to find the rc files

				final FileSystem fs = inputPath.getFileSystem(ctx
						.getConfiguration());

				if (fs.getFileStatus(inputPath).isDir()) {
					// assure that we are at the root of the partition tree.
					FileStatus fileStatusArr[] = fs.listStatus(inputPath);

					if (fileStatusArr != null) {
						for (FileStatus childFileStatus : fileStatusArr) {
							final FileStatus childFileStatusF = childFileStatus;

							service.submit(new TreeCallable<String>(callable) {

								public String run() throws Exception {

									getPartitionedFiles(expressionFactory,
											partitionExpression, fs,
											childFileStatusF, 0, partitionKeys,
											splitPaths, service, this);

									return null;

								}
							});

						}
					}

				} else {
					if (filter.accept(null, inputPath.toString()))
						splitPaths.add(fs.getFileStatus(inputPath));
				}

			}

			// wait for completion
			callable.waitForNodeThenChildren();

			service.shutdown();
			
			if (splitPaths.size() < 1) {
				LOG.error("No split paths where found, please check that the filter logic for the partition keys does not filter out everything ");
			}

		} else {

			for (Path path : inputPaths) {
				PathUtil.listRecursive(path, ctx.getConfiguration(), splitPaths);
			}

		}

		return splitPaths;
	}

	/**
	 * Recursively works through all directories, skipping filtered partitions.
	 * 
	 * @param fs
	 * @param fileStatus
	 * @param partitionLevel
	 * @param partitionKeys
	 * @param splitPaths
	 * @throws IOException
	 */
	private void getPartitionedFiles(final ExpressionFactory expressionFactory,
			final String partitionExpression, final FileSystem fs,
			FileStatus fileStatus, int partitionLevel,
			final String[] partitionKeys, final Collection<FileStatus> splitPaths,
			final ExecutorService service, final TreeCallable<String> callable)
			throws IOException {

		final Path path = fileStatus.getPath();
		final int partitionPathLen = path.toString().split("=").length;

		// filter out hidden files
		if (path.getName().startsWith("_")) {
			return;
		}

		// this means we are inside the partition so that the path will
		// contain all partitions plus its values
		// we can apply the partition filter expression here that was passed
		// to the HiveColumnarLoader.setPartitionExpression
		if (partitionPathLen >= partitionKeys.length
				&& !evaluatePartitionExpression(expressionFactory,
						partitionExpression, path)) {

			LOG.debug("Pruning partition: " + path);
			return;

		}

		// after this point we now that the partition is either null
		// which means we are at the end of the partition tree and all files
		// sub directories should be included.
		// or that we are still navigating the partition tree.

		// iterate over directories if fileStatus is a dir.

		if (fileStatus.isDir()) {

			final FileStatus[] childStatusArr;

			if ((childStatusArr = fs.listStatus(path)) != null) {
				for (final FileStatus childFileStatus : childStatusArr) {

					service.submit(new TreeCallable<String>(callable) {
						@Override
						public String run() throws Exception {
							
							getPartitionedFiles(expressionFactory,
									partitionExpression, fs, childFileStatus,
									0, partitionKeys, splitPaths, service, this);

							return null;
						}
					});

				}
			}

		} else {
			// add file to splitPaths
			if(fileStatus != null)
				splitPaths.add(fileStatus);
		}
	}

	/**
	 * Evaluates the partitionExpression set in the
	 * HiveColumnarLoader.setPartitionExpression. * @
	 * 
	 * @param partitionExpression
	 *            String
	 * @param path
	 *            Path
	 * @return boolean
	 * @throws IOException
	 */
	private boolean evaluatePartitionExpression(
			ExpressionFactory expressionFactory, String partitionExpression,
			Path path) throws IOException {

		boolean ret = true;

		if (expressionFactory != null) {
			if (!partitionExpression.startsWith("${")) {
				partitionExpression = "${" + partitionExpression + "}";
			}

			Map<String, String> context = pathPartitioner
					.getPathPartitionKeyValues(path.toString());

			MapVariableMapper mapper = new MapVariableMapper(expressionFactory,
					context);
			VariableContext varContext = new VariableContext(mapper);

			ValueExpression evalExpression = expressionFactory
					.createValueExpression(varContext, partitionExpression,
							Boolean.class);

			ret = (Boolean) evalExpression.getValue(varContext);

			LOG.debug("Evaluated: " + partitionExpression + " returned: " + ret);

		}

		return ret;
	}

	/**
	 * 
	 * ELContext implementation containing the VariableMapper MapVariableMapper
	 * 
	 */
	class VariableContext extends ELContext {

		VariableMapper variableMapper;

		VariableContext(VariableMapper variableMapper) {
			this.variableMapper = variableMapper;
		}

		@Override
		public ELResolver getELResolver() {
			return null;
		}

		@Override
		public FunctionMapper getFunctionMapper() {
			return null;
		}

		@Override
		public VariableMapper getVariableMapper() {
			return variableMapper;
		}

	}

	/**
	 * Implementation for the VariableMapper that takes the values in a Map and
	 * creates ValueExpression objects for each.
	 * 
	 */
	class MapVariableMapper extends VariableMapper {
		private Map<String, ValueExpression> valueExpressionMap;

		public MapVariableMapper(ExpressionFactory expressionFactory,
				Map<String, String> variableMap) {

			valueExpressionMap = new HashMap<String, ValueExpression>();

			for (Entry<String, String> entry : variableMap.entrySet()) {
				ValueExpression valExpr = expressionFactory
						.createValueExpression(entry.getValue(), String.class);
				valueExpressionMap.put(entry.getKey(), valExpr);
			}

		}

		@Override
		public ValueExpression resolveVariable(String variableName) {
			return valueExpressionMap.get(variableName);
		}

		@Override
		public ValueExpression setVariable(String variableName,
				ValueExpression valueExpression) {
			return valueExpressionMap.put(variableName, valueExpression);
		}

	}

	public class TreeCallable<T> implements Callable<T> {

		final CountDownLatch latch = new CountDownLatch(1);

		final transient List<TreeCallable<T>> children = new ArrayList<TreeCallable<T>>();

		public TreeCallable(TreeCallable<T> parent) {
			if (parent == null) {
				latch.countDown();
			} else {
				parent.add(this);
			}
		}

		private void waitForChildren() throws InterruptedException,
				ExecutionException {
			for (TreeCallable<T> child : children) {
				child.waitForNodeThenChildren();
			}
		}

		public void waitForNodeThenChildren() throws InterruptedException,
				ExecutionException {
			latch.await();
			waitForChildren();
		}

		public void add(TreeCallable<T> callable) {
			children.add(callable);
		}

		public T run() throws Exception {
			return null;
		}

		public T call() throws Exception {
			try {
				return run();
			} finally {
				latch.countDown();
			}
		}

	}

}

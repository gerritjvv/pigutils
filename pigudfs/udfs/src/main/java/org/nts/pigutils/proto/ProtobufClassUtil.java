package org.nts.pigutils.proto;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.protobuf.Message;

/**
 * 
 * Load the protobuf user class from the class path
 * 
 */
public class ProtobufClassUtil {

	private static final Logger LOG = Logger.getLogger(ProtobufClassUtil.class);

	/**
	 * 
	 * @param clsMapping
	 * @param conf
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends Message> loadProtoClass(String clsMapping,
			Configuration conf) {

		String protoClassName = conf.get(clsMapping);

		if (protoClassName == null) {
			throw new RuntimeException("No property defined for " + clsMapping);
		}

		Class<? extends Message> protoClass = null;

		try {
			protoClass = (Class<? extends Message>) Thread.currentThread()
					.getContextClassLoader().loadClass(protoClassName);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error instantiating " + protoClassName
					+ " referred to by " + clsMapping, e);
		}

		//LOG.info("Using " + protoClass.getName() + " mapped by " + clsMapping);

		return protoClass;

	}

}
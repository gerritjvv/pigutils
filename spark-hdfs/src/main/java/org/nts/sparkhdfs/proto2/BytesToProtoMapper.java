package org.nts.sparkhdfs.proto2;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

/**
 * Mapper function to map a JavaRDD bytebuff bytes to GPB Message
 * 
 * @param <R>
 */
public class BytesToProtoMapper implements
		Function<Tuple2<Writable, BytesWritable>, Message> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Message protoClass;

	private BytesToProtoMapper(Class<? extends MessageOrBuilder> protoClass)
			throws InstantiationException, IllegalAccessException {
		try {
			this.protoClass = (Message) protoClass.getMethod(
					"getDefaultInstance", new Class[0]).invoke(null, null);
		} catch (Exception exc) {
			RuntimeException rte = new RuntimeException(exc);
			rte.setStackTrace(exc.getStackTrace());
			throw rte;
		}
	}

	public static final BytesToProtoMapper newInstance(
			Class<? extends MessageOrBuilder> protoClass)
			throws InstantiationException, IllegalAccessException {
		return new BytesToProtoMapper(protoClass);
	}

	public Message call(Tuple2<Writable, BytesWritable> tuple) throws Exception {
		return protoClass.newBuilderForType().mergeFrom(tuple._2.copyBytes())
				.build();
	}

}

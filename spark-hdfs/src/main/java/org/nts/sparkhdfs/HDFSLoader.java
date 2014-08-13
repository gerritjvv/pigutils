package org.nts.sparkhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.nts.sparkhdfs.proto2.BytesToProtoMapper;
import org.nts.sparkhdfs.proto2.LZOProtoInputFormat;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

public class HDFSLoader {

	private final JavaSparkContext sparkCtx;
	private final Configuration conf;

	private HDFSLoader(JavaSparkContext sparkCtx, Configuration conf) {
		this.sparkCtx = sparkCtx;
		this.conf = conf;
	}

	public final JavaPairRDD<Writable, BytesWritable> lzoHadoopRDD(String path,
			Configuration conf) {
		return sparkCtx.newAPIHadoopFile(path, LZOProtoInputFormat.class,
				Writable.class, BytesWritable.class, conf);
	}

	public final JavaRDD<Message> loadLzoProto2Messages(String path,
			Class<? extends MessageOrBuilder> messageCls)
			throws InstantiationException, IllegalAccessException {
		return lzoHadoopRDD(path, conf).map(
				BytesToProtoMapper.newInstance(messageCls));
	}

	public static final HDFSLoader newInstance(JavaSparkContext sparkCtx,
			Configuration conf) {
		return new HDFSLoader(sparkCtx, conf);
	}
}

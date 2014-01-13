package hiveutils.serde.gpb;

import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

public class Base64GPBSerde extends AbstractSerDe {

	public static final String PROP_GPB_CLASS = "gpb.class";

	IFn parser;
	@SuppressWarnings("rawtypes")
	Class protoClass;

	SerDeStats stats;
	ObjectInspector io;

	@Override
	public Object deserialize(Writable w) throws SerDeException {
		// we expect a message to be Base64(GPB(msg)) encoded.

		final Text txt = (Text) w; // must be text
		final byte[] bts = new Base64(0).decode(txt.toString()); // decode
																	// message

		// return a deserialized GPB message
		return parser.call(bts);

	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return io;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return stats;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return GPBSerialized.class;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void initialize(Configuration arg0, Properties prop)
			throws SerDeException {

		stats = new SerDeStats();

		String clsName = prop.getProperty(PROP_GPB_CLASS);
		try {
			protoClass = Thread.currentThread().getContextClassLoader()
					.loadClass(clsName);
			final Method m = protoClass.getMethod("parseFrom", byte[].class);

			io = new JavaObjectInspector((Descriptor) protoClass.getMethod(
					"getDescriptor").invoke(null));

			parser = new IFn() {

				public Object call(Object... args) throws RuntimeException {
					try {
						return GPBTransform.convert((Message) m.invoke(null,
								args));
					} catch (Throwable t) {
						throwAsRuntime(t);
						return null;
					}
				}
			};

		} catch (Throwable e) {
			throw new RuntimeException(e.toString(), e);
		}

	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1)
			throws SerDeException {
		return null;
	}

	private static final void throwAsRuntime(Throwable t)
			throws RuntimeException {
		RuntimeException rte = new RuntimeException(t.toString(), t);
		rte.setStackTrace(t.getStackTrace());
		throw rte;
	}

	static interface IFn {
		public Object call(Object... args) throws RuntimeException;
	}

}

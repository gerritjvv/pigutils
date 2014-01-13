package hiveutils.serde.gpb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;

/**
 * 
 * Returns the data from the hashmap returned in the Base64GPBSerde.
 * Hive is not able to read the hashtable data, so we return it here, and 
 * present it to hive as a Struct.
 * 
 */
public class JavaObjectInspector extends StandardStructObjectInspector{

	
	protected JavaObjectInspector(Descriptor d) {
		super(getFields(d));
	}


	@Override
	public boolean isSettable() {
		return false;
	}
	
	

	@SuppressWarnings("unchecked")
	@Override
	public Object getStructFieldData(Object data, StructField fieldRef) {
		final Map<String, Object> m = (Map<String,Object>)data;
		
		return m.get(fieldRef.getFieldName());
	}


	@SuppressWarnings("unchecked")
	@Override
	public List<Object> getStructFieldsDataAsList(Object data) {
		final Map<String, Object> m = (Map<String,Object>)data;
		
		final List<Object> l = new ArrayList<Object>();
		for(Object v : m.values())
			l.add(v);
		
		return l;
	}


	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}


	/**
	 * Creates a list of ObjectField(s) that each has its object inspector.
	 * @param map
	 * @return
	 */
	private static final List<StructField> getFields(Descriptor msgd){
		List<StructField> fields = new ArrayList<StructField>();
		
		for (FieldDescriptor d : msgd.getFields()) {

			if (d.isRepeated()) {
				fields.add(new RepeatedField(d));
			}else{
				fields.add(new ObjectField(d));
			}
		}

		
		return fields;
	}
	
	static public class RepeatedField implements StructField{
		final String name;
		final ObjectInspector io;
		
		public RepeatedField(FieldDescriptor fd){
			this.name = fd.getName();
			
			final ObjectField of = new ObjectField(fd);
			io = ObjectInspectorFactory.getStandardListObjectInspector(of.getFieldObjectInspector());
		}

		@Override
		public String getFieldName() {
			return name;
		}

		@Override
		public ObjectInspector getFieldObjectInspector() {
			return io;
		}

		@Override
		public String getFieldComment() {
			return "";
		}
		
	}
	
	/**
	 * 
	 * Selects the correct ObjectInspector based on the val type.
	 * 
	 */
	static public class ObjectField implements StructField{

		final String name;
		final ObjectInspector io;
		
		public ObjectField(FieldDescriptor fd){
			this.name = fd.getName();
			
			final Type o = fd.getType();
			if(o == Type.MESSAGE){
               io = new JavaObjectInspector(fd.getMessageType());
			}else{
				if(o == Type.BOOL)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Boolean.class);
				else if(o == Type.BYTES)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(byte[].class);
				else if(o == Type.DOUBLE)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Double.class);
				else if(o == Type.FIXED32 || o == Type.INT32 || o == Type.SFIXED32 || o == Type.SINT32 || o == Type.UINT32)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Integer.class);
				else if(o == Type.FIXED64 || o == Type.INT64 || o == Type.SFIXED64 || o == Type.SINT64 || o == Type.UINT64)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Long.class);
				else if(o == Type.FLOAT)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(Float.class);
				else if(o == Type.STRING || o == Type.ENUM)
					io = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(String.class);
				else
					throw new RuntimeException("The type " + o + " is not supported");
			}
			
		}
		
		@Override
		public String getFieldName() {
			return name;
		}

		@Override
		public ObjectInspector getFieldObjectInspector() {
			return io;
		}

		@Override
		public String getFieldComment() {
			return "";
		}
		
	}
	
}

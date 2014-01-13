package hiveutils;

import hiveutils.serde.gpb.Base64GPBSerde;

import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.example.tutorial.AddressBookProtos.AddressBook;
import com.example.tutorial.AddressBookProtos.AddressBook.Builder;
import com.example.tutorial.AddressBookProtos.Person;


public class AppTest{
	
	
	@Test
	public void testGPBToMap() throws Exception{
		
	   Builder abBuilder = AddressBook.newBuilder();
	   for(int i = 0; i < 5; i++)
		   abBuilder.addPerson(createPerson());
	   
		AddressBook ab = abBuilder.build();
	
		
		Text txt = new Text(new Base64(0).encodeAsString(ab.toByteArray()));
		
		Properties props = new Properties();
		props.setProperty(Base64GPBSerde.PROP_GPB_CLASS, AddressBook.class.getName());
		
		
		//Map<String, Object> m = GPBTransform.convert(ab);
	    //System.out.println(m);
		Base64GPBSerde serde = new Base64GPBSerde();
		serde.initialize(null, props);
		
		StandardStructObjectInspector oi = (StandardStructObjectInspector) serde.getObjectInspector();
		Object obj = serde.deserialize(txt);
		
		System.out.println("OI: " + oi);
		System.out.println("obj : " + obj);
		
		StructField ref = oi.getStructFieldRef("person");
		System.out.println("Ref: " + ref);
		Object person_data = oi.getStructFieldData(obj, ref);
		System.out.println("person_data: " + person_data);
	}

	private Person createPerson() {
		return Person.newBuilder().setEmail("test@gmail.com")
				.setId((int)System.currentTimeMillis())
				.setName("test").build();
	}
	
	
}
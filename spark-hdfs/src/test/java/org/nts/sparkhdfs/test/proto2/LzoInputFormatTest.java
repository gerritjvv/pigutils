package org.nts.sparkhdfs.test.proto2;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;
import org.anarres.lzo.hadoop.codec.LzoCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nts.sparkhdfs.HDFSLoader;

import util.Person;
import util.Person.person;

import com.google.protobuf.Message;

/**
 * This test case uses the org.anarres java lzo library for creating and reading the lzo test files. This does not mean that the InputFormat or code requires the java library.<br/>
 * The LzoProtoInputFormat will use any codec specified for the file provided.
 */
public class LzoInputFormatTest {

	static Configuration conf = new Configuration();
	static MiniDFSCluster hdfsCluster;
	static String hdfsURI;
	static JavaSparkContext sparkCtx;

	static FileSystem fs;

	@BeforeClass
	public static void setup() throws IOException {
		File baseDir = new File("./target/tests/hdfs/lzoinputformattests")
				.getAbsoluteFile();
		FileUtil.fullyDelete(baseDir);
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();
		hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";

		conf.set("fs.default.name", hdfsURI);
		conf.set("io.compression.codecs", LzoCodec.class.getCanonicalName());

		fs = FileSystem.get(conf);

		sparkCtx = new JavaSparkContext("local", "test");
		System.out.println("HDFS URI: " + hdfsURI);
	}

	@AfterClass
	public static void shutdown() {
		hdfsCluster.shutdown();
		sparkCtx.stop();
	}

	@Test
	public void testLzoInputformat() throws Exception {
		// write out lzo test file and copy to hdfs
		File testFile = new File("target/tests/lzoinputformattests/input.lzo");
		testFile.getParentFile().mkdirs();
		testFile.createNewFile();

		writeTestFile(testFile, 10);

		Path hdfsPath = new Path("/test/mytest.lzo_deflate");

		fs.copyFromLocalFile(new Path(testFile.getAbsolutePath()), hdfsPath);

		// do spark tests
		JavaRDD<Message> data = HDFSLoader
				.newInstance(sparkCtx, conf)
				.loadLzoProto2Messages(hdfsPath.toString(), Person.person.class);

		assertEquals(10, data.count());

	}

	@SuppressWarnings("unused")
	private static final Person.person[] readFile(File file) throws Exception {
		List<Person.person> people = new ArrayList<Person.person>();
		LzoDecompressor decomp = LzoLibrary.getInstance().newDecompressor(
				LzoAlgorithm.LZO1X, null);

		DataInputStream in = new DataInputStream(new LzoInputStream(
				new FileInputStream(file), decomp));
		BytesWritable writes = new BytesWritable();
		try {
			while (true) {
				writes.readFields(in);
				people.add(Person.person.parseFrom(writes.copyBytes()));
			}
		} catch (EOFException eof) {
			// ignore
		} finally {
			in.close();
		}

		return people.toArray(new Person.person[0]);
	}

	private static final void writeTestFile(File file, int n)
			throws IOException {

		// we use the java lzo implementatio to write an lzo test file
		DataOutputStream out = new DataOutputStream(new LzoOutputStream(
				new FileOutputStream(file)));
		BytesWritable writes = new BytesWritable();
		try {
			for (int i = 0; i < n; i++) {
				final byte[] bts = person.newBuilder().setAge(i * 10)
						.setName("name-" + i).build().toByteArray();
				writes.set(bts, 0, bts.length);
				writes.setSize(bts.length);
				writes.write(out);
			}
		} finally {
			out.close();
		}

	}

}

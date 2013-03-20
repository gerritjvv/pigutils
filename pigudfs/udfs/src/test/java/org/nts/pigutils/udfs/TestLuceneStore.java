package org.nts.pigutils.udfs;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLuceneStore {
	
	
	private static Cluster cluster;
	private PigTest test;
	
	
	@Test
	public void testDocumentWrite() throws IOException, ParseException{
		
		FileUtils.deleteDirectory(new File("target/luceneindex"));
		
		String[] script = {
				"r = load 'luceneinput.csv' as (lbl:chararray,desc:chararray,score:int);",
				"store r into 'target/luceneindex' using org.nts.pigutils.lucene.LuceneStore();"
		};
		
		test = new PigTest(script);
		test.unoverride("STORE");
		test.runScript();
		
		IndexReader reader = IndexReader.open(FSDirectory.open(new File("target/luceneindex/index-0")));
		
		assertEquals(4, reader.maxDoc());
		
		
	}

	@BeforeClass
	public static void setup() throws Throwable{
		
		cluster = PigTest.getCluster();
		cluster.update(new Path("src/test/resources/lucenestoredat.txt"), new Path("luceneinput.csv"));
		
	}

	
}

#Pig Utils

This library is freely distributed on the Apache2 Licence.

Its a collection of Pig UDFS, Loaders and Stores providing functionality on top of the current pig libraries.

#UDFS

* TO_MAP
  org.nts.pigutils.udfs.TO_MAP(input)
  
  Converts a DataBag or Tuple into a HashMap

  The Tuple or Tuples in the Bag is expected to have between one and two keys.

  If the Tuple has:<br/>
  One entry, the entry is placed as a key and the value as null.<br/>
  Two entries, the first is taken as the key and the second as the value.<br/>
  Three or more, the first is taken as the key and the rest placed as a Tuple and taken as the value.<br/>
 
  Important, keys are always converted to Strings.

* DateFormat
  org.nts.pigutils.udfs.DateFormat(tsMillis, 'formatstring')
  
  The formatstring is the same as for Java's SimpleDateFormat 
  e.g. 'yyyy-MM'dd' will format a timestamp to year month and day

* PurifyInt
  org.nts.udfs.PurifyInt(strInteger)

  Takes a String, removes any non integer characters and returns an integer

#LZO GPB Loader

* LZO Protobuf Base64 Loader
 
 org.nts.pigutils.proto.LzoProtobuffB64LinePigStore('gpbkey')
  
  The gpbkey should be configured in your pig.properties file as a key and the value should point to the actual protobuff java class

# SOLR Store

* Store data to a SOLR Cloud Server
  
  Example
```
  l = load '/opt/solr/exampledocs/pigbooks.csv' using PigStorage(',') as 
	(id:chararray,cat:chararray,name:chararray,price:double,
	inStock:chararray,author:chararray,series_t:chararray,sequence_i:chararray,
	genre_s:chararray);

	
  store l into '/tmp/abc' using org.nts.pigutils.lucene.SolrCloudStore('localhost:9983', 'collection1');	 
```

# Lucene Index Create Store

* Create Lucene Indexes using Pig

  Exmaple
```
   r = load 'luceneinput.csv' as (lbl:chararray,desc:chararray,score:int);
   store r into 'target/luceneindex' using org.nts.pigutils.lucene.LuceneStore();
```


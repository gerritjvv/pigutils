#Overview

Spark HDFS ProtoBuf LZO library.

##


## Usage

```java

  JavaRDD<Message> data = HDFSLoader
		           .newInstance(sparkCtx, conf)
			   .loadLzoProto2Messages(hdfsPath.toString(), Person.person.class);
```

#Licence

Distributed under the Eclipse Public License either version 1.0

Note that we only use the LZO library for testing. The rest of the code that is deployed use the hadoop compression codec classes and thus
is technically LZO independant, and does not have to be the same license as lzo which is gnu general public.


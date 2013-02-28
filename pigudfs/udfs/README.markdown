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


#LZO GPB Loader

* LZO Protobuf Base64 Loader
 
 org.nts.pigutils.proto.LzoProtobuffB64LinePigStore('gpbkey')
  
  The gpbkey should be configured in your pig.properties file as a key and the value should point to the actual protobuff java class
  

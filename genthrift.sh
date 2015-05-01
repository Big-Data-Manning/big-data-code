rm -rf gen-javabean src/java/manning/schema
thrift -r --gen java:beans,hashcode,nocamel src/schema.thrift
mv gen-javabean/manning/schema src/java/manning/
rm -rf gen-javabean

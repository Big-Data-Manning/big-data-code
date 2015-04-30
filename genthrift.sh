rm -rf gen-javabean src/jvm/manning/schema
thrift -r --gen java:beans,hashcode,nocamel src/schema.thrift
mv gen-javabean/manning/schema src/jvm/manning/
rm -rf gen-javabean

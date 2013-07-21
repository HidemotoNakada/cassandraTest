CAS=/home/nakada/cassandra
CLASSPATH=bin:${CAS}/build/apache-cassandra-thrift-1.2.4-SNAPSHOT.jar:${CAS}/lib/libthrift-0.7.0.jar:${CAS}/lib/slf4j-api-1.7.2.jar:lib/cassandra-jdbc-1.2.5.jar:${CAS}/build/apache-cassandra-1.2.4-SNAPSHOT.jar:${CAS}/lib/guava-13.0.1.jar

cqlTest:
	java -cp ${CLASSPATH} test.CQLTest

cqlSelect:
	java -cp ${CLASSPATH} test.CQLTest select

put:
	java -cp ${CLASSPATH} test.App6 testKS cfname key col testFile

get:
	java -cp ${CLASSPATH} test.App5 testKS cfname key col testFile

get2:
	java -cp ${CLASSPATH} test.App8 testKS cfname key col testFile


jar:
	cd bin; jar -cf ../udf.jar test

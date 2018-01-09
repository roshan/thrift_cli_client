# Thrift CLI Client

A short Java CLI client to use with Thrift generated code to query an arbitrary service.

## Using

1. Clone the repo `git clone git@github.com:roshan/thrift_cli_client.git`

2. Create the JAR `mvn clean package`

3. Start the JAR with your Java Thrift-generated code on the classpath `java -cp your.jar:thrift_cli_client.jar com.arjie.thrift_cli_client.ThriftCliClient host port fully.qualified.class.Name\$Client method_name`. Optionally, redirect input to it from a file.

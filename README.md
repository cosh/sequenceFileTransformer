# sequenceFileTransformer
A simple spring boot command line runner to transform hbase sequence files into multiple json files

# build & package
```
mvn clean package
```

# run
```
java -jar uberJar --sourceFile=... --destinationPath=... batchSize=1000000
```

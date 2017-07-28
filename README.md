# light elephant bird

I extracted three modules from twitter's [elephant-bird](https://github.com/twitter/elephant-bird) (i.e., core, hadoopcompat and examples) which aims to conveniently upgrade the versions of protobuf and thrift in simple development enviroment.

Here I set protobuf version to 3.3.0 and thrift version to 0.10.0.

I also wrote a [spark example](src/test/java/io/github/qf6101/TestSparkProtoIO.java) for writing and reading the protobuf data.


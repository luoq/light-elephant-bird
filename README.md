# light elephant bird

I extract 3 modules from twitter's [elephant-bird](https://github.com/twitter/elephant-bird) (i.e., core, hadoopcompat and examples).

This aims to lightly update the protobuf and thrift version in simple development enviroment.

Here I update the protobuf to version 3.3.0 and thrift to version 0.10.0.

I also write an [spark example](src/test/java/io/github/qf6101/TestSparkProtoIO.java) for writing and read the protobuf data.


package io.github.qf6101;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.examples.proto.AddressBookProtos.Person;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by qfeng on 17-7-27.
 */
public class TestSparkProtoIO {
    public static void main(String[] args) throws IOException {
        // initialize spark session and context
        SparkSession spark = SparkSession
                .builder()
                .appName("test_spark_proto_io")
                .master("local")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // set lzo and protobuf info to hadoop configuration
        Configuration conf = sc.hadoopConfiguration();
        conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
        conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        LzoProtobufBlockOutputFormat.setClassConf(Person.class, sc.hadoopConfiguration());
        MultiInputFormat.setClassConf(Person.class, conf);
        // create protobuf data
        Person alice = Person.newBuilder().setName("Alice").setId(1).setEmail("alice@163.com")
                .addPhone(Person.PhoneNumber.newBuilder().setNumber("0571789546874").build()).build();
        Person bob = Person.newBuilder().setName("Bob").setId(2).setEmail("bob@163.com")
                .addPhone(Person.PhoneNumber.newBuilder().setNumber("01045879546").build()).build();
        // delete the exist output directory
        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
        if (fs.exists(new Path("people_info"))) fs.delete(new Path("people_info"), true);
        // write protobuf data
        sc.parallelize(Lists.newArrayList(alice, bob))
                .mapToPair((PairFunction<Person, LongWritable, ProtobufWritable<Person>>) person -> {
                    ProtobufWritable<Person> personProto = ProtobufWritable.newInstance(Person.class);
                    personProto.set(person);
                    return new Tuple2<>(new LongWritable(0), personProto);
                })
                .saveAsNewAPIHadoopFile("people_info",
                        new LongWritable(0).getClass(),
                        new ProtobufWritable<Person>().getClass(),
                        new LzoProtobufBlockOutputFormat<Person>().getClass(),
                        sc.hadoopConfiguration());
        // read protobuf data
        sc.newAPIHadoopFile("people_info",
                new MultiInputFormat<Person>().getClass(),
                LongWritable.class,
                BinaryWritable.class,
                conf).foreach((VoidFunction<Tuple2<LongWritable, BinaryWritable>>) data ->
                System.out.println(((Person) data._2().get()).getId()));
    }
}

package com.westar.hbase.storage;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 //1 ： 创建HBase表
 create 'sensor', {NUMREGIONS => 2, SPLITALGO => 'HexStringSplit'}, {NAME => 'v', COMPRESSION => 'NONE', BLOOMFILTER => 'NONE'}

 //2：将csv文件的数据上传到HDFS中
 hadoop fs -put omneo.csv hdfs://slave2:8020/user/hadoop/hbase-course/20180507

 //3：将CSV文件转成HFile文件
 hadoop jar practice-1.0-SNAPSHOT-jar-with-dependencies.jar com.twq.hbase.storage.ConvertToHFile \
 hdfs://slave2:8020//user/hadoop/hbase-course/20180507/omneo.csv \
 hdfs://slave2:8020/user/hadoop/hbase-course/hfiles/20180507 sensor

 //4：将转成的HFi了文件导入到HBase中
 hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/hadoop/hbase-course/hfiles/20180507 sensor

 //5：验证数据
 count 'sensor', INTERVAL=>40000,CACHE=>40000
 scan 'sensor', {LIMIT => 1}
 */
public class ConvertToHFile {
    public static class ConvertToHFilesMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {
        public static final byte[] CF = Bytes.toBytes("v");

        public static final EncoderFactory encoderFactory = EncoderFactory.get();
        public static final ByteArrayOutputStream out = new ByteArrayOutputStream();
        public static final DatumWriter<Event> writer = new SpecificDatumWriter<Event>(Event.getClassSchema());
        public static final BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
        public static final Event event = new Event();
        public static final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(","); // <1> 将csv文件中的每行数据按照逗号切割

            event.setId(line[0]);
            event.setEventId(line[1]);
            event.setEventType(line[2]);
            event.setPartName(line[3]);
            event.setPartNumber(line[4]);
            event.setVersion(Long.parseLong(line[5]));
            event.setPayload(line[6]);  // <2> 我们每次使用一个event来进行值的传递，以减少开销，复用event对象

            // Serialize the AVRO object into a ByteArray
            out.reset(); // <3> 复用out对象，以减少开销
            writer.write(event, encoder); // <4> 复用writer, 将event对象序列化成二进制
            encoder.flush();

            byte[] rowKeyBytes = DigestUtils.md5(line[0]);
            rowKey.set(rowKeyBytes); // <5> 设置rowkey，使用传感器id的md5作为rowkey
            // <6> 构造HFile中的KeyValue对象
            KeyValue kv = new KeyValue(rowKeyBytes, CF, Bytes.toBytes(line[1]), out.toByteArray());
            context.write (rowKey, kv); // <7> 将rowkey和keyvalue写到HFile中
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        String inputPath = "hdfs://slave2:8020/user/hadoop/hbase-course/20180507/omneo.csv";
        String outputPath = "hdfs://slave2:8020/user/hadoop/hbase-course/hfiles/20180507";
        TableName tableName =TableName.valueOf("sensor");
//        String inputPath = args[0];
//        String outputPath = args[1];
//        TableName tableName = TableName.valueOf(args[2]);

        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(tableName);

        Job job = Job.getInstance(configuration, "ConvertToHFile");
        //HBase提供的大部分配置
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));
        job.setInputFormatClass(TextInputFormat.class);//我们读取的是csv文件

        job.setJarByClass(ConvertToHFile.class);
        job.setMapperClass(ConvertToHFilesMapper.class); // <4>
        job.setMapOutputKeyClass(ImmutableBytesWritable.class); // <5>
        job.setMapOutputValueClass(KeyValue.class); // <6>

        FileInputFormat.setInputPaths(job, inputPath);
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));


        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(1);
        }

    }
}

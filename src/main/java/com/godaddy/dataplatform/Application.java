package com.godaddy.dataplatform;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class Application extends Configured implements Tool {

  public static class Map extends Mapper<WritableComparable, HCatRecord, Text, IntWritable> {
    HCatSchema schema ;
    Text host = new Text();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {

      schema = HCatInputFormat.getTableSchema(context.getConfiguration());
      if (schema == null) {
        throw new RuntimeException("schema is null");
      }

    }
    @Override
    protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
      // another way, get data by column index
      // host.set(value.get(8));
      host.set(value.getString("name", schema));
      context.write(host, new IntWritable(1));
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, WritableComparable, HCatRecord> {
    HCatSchema schema;
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException {
      schema = HCatOutputFormat.getTableSchema(context.getConfiguration());
    }
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        // Just doing something random.
        sum++;
        iter.next();
      }
      HCatRecord record = new DefaultHCatRecord(2);
      record.setString("original_name", schema, key.toString());
      record.setInteger("value", schema, sum);

      context.write(null, record);
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Application(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {

    // Set the Db name.
    String dbName = "c360_glue_database";
    // Set the input table name.
    String inputTableName = "read";
    // Set the output table name.
    String outputTableName = "write";

    // Setting job properties.
    Job job = Job.getInstance();
    job.setInputFormatClass(HCatInputFormat.class);
    job.setJarByClass(Application.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    job.setNumReduceTasks(2);

    Configuration conf = job.getConfiguration();
    // Set the input format.
    HCatInputFormat.setInput(conf,dbName,inputTableName);

    // Set the output format.
    HCatOutputFormat.setOutput(job,OutputJobInfo.create(dbName,outputTableName,new HashMap<>()));
    job.setOutputFormatClass(HCatOutputFormat.class);

    return (job.waitForCompletion(true)?0:1);
  }
}

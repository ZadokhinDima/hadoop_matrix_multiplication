import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplicationJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MatrixMultiplicationJob.class);
        job.setMapperClass(MatrixMultiplicationMapper.class);

        // Chain the first reducer
        ChainReducer.setReducer(job, MultiplicationReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        // Add an intermediate mapper (identity mapper)
        ChainMapper.addMapper(job, IdentityMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        // Chain the second reducer
        ChainReducer.setReducer(job, SumReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration(false));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
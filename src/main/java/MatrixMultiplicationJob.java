import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplicationJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1
        Job job1 = Job.getInstance(conf, "Multiplication phase");
        job1.setJarByClass(MatrixMultiplicationJob.class);
        job1.setMapperClass(MatrixMultiplicationMapper.class);
        job1.setReducerClass(MultiplicationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediatePath = new Path("/intermediate");
        FileOutputFormat.setOutputPath(job1, intermediatePath);

        // Run the first job
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2
        Job job2 = Job.getInstance(conf, "Summation Phase");
        job2.setJarByClass(MatrixMultiplicationJob.class);
        job2.setMapperClass(IdentityMapper.class);
        job2.setReducerClass(SumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        // Run the second job
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
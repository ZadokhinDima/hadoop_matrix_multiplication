import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException{
        int sum = 0;
        for (Text value : values) {
            int val = Integer.parseInt(value.toString());
            sum += val;
        }
        context.write(new Text("C," + key.toString()), new Text(String.valueOf(sum)));
    }
}

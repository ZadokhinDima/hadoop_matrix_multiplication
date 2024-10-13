
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer class for the matrix multiplication job.
 * This reducer will receive the output from the mapper, which will be in the form of:
 * Key: Row number of A or Column number of B
 * Value: [Matrix Name, Column A / Row B, Value]
 * This reducer will multiply the values from A and B, and emit the result as the key, and the value as the result of the multiplication.
 */
public class MultiplicationReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<IntermediateNode> fromA = new ArrayList<>();
        List<IntermediateNode> fromB = new ArrayList<>();

        for (Text value : values) {
            IntermediateNode node = new IntermediateNode(key, value);
            if (node.matrixName.equals("A")) {
                fromA.add(node);
            } else {
                fromB.add(node);
            }
        }

        for (IntermediateNode a : fromA) {
            for (IntermediateNode b : fromB) {
                int result = a.val * b.val;
                context.write(new Text(a.secondaryIndex + "," + b.secondaryIndex), new IntWritable(result));
            }
        }

    }

    static class IntermediateNode {
        String matrixName;
        int primaryIndex;
        int secondaryIndex;
        int val;

        IntermediateNode(Text key, Text value) {
            String[] tokens = value.toString().split(",");
            primaryIndex = Integer.parseInt(key.toString());
            matrixName = tokens[0];
            secondaryIndex = Integer.parseInt(tokens[1]);
            val = Integer.parseInt(tokens[2]);
        }
    }
}

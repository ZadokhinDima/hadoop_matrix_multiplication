import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * Matrices are stored in format A,0,0,1 (Matrix A, row 0, column 0, value 1)
     * Mappers job is to emit [row number of A or column number of B] as key, and [other index, value] as value
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Mapper input: " + value.toString());
        String[] tokens = value.toString().split(",");
        String matrixName = tokens[0];  // A or B
        int row = Integer.parseInt(tokens[1]);
        int col = Integer.parseInt(tokens[2]);
        int val = Integer.parseInt(tokens[3]);

        if (matrixName.equals("A")) {
            context.write(new Text(String.valueOf(col)), new Text( matrixName + ',' + row + "," + val));
        } else {
            context.write(new Text(String.valueOf(row)), new Text( matrixName + ',' + col + "," + val));
        }

    }
}

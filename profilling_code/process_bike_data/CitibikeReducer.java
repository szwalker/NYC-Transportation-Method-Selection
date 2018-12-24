import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CitibikeReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
        double count = 0;
        double sum = 0;
        for(Text value: values){
            sum += Double.parseDouble(value.toString());
            count++;
        }
        double average = sum/count;
        //try to get standard deviation here:
        
        
        double variance = 0.0;
        
        for(Text value: values){
            double curVal= Double.parseDouble(value.toString());
            variance += Math.pow(curVal-average,2);
        }
        
        double standardDeviation = Math.sqrt(variance/count);
        
        double newCount = 0;
        double newSum = 0;
        
        for(Text value: values){
            double curVal = Double.parseDouble(value.toString());
            if((curVal<(average+(2.0*standardDeviation)))&&(curVal>(average-(2.0*standardDeviation)))){
                newCount++;
                newSum+=curVal;
            }
        }
        
        double newAverage = newSum/newCount;
        //String formatString =  Double.toString(newAverage)+","+Double.toString(newCount);
        String result = Double.toString(variance);
        context.write(key,new Text(variance));
    }
}
//standard deviation is the problem

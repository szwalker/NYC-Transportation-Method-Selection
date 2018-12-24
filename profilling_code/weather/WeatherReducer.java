import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WeatherReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
        int count = 0;
        String rain = null;
        String snow = null;
        for(Text value: values){
            if(count==0){//rain
                rain = value.toString();
            }else{
                snow = value.toString();
            }
            count++;
        }
        double rain_dou = Double.parseDouble(rain);
        double snow_dou = Double.parseDouble(snow);
        if(rain_dou>0&&snow_dou>0)
            context.write(key,new Text("1"));
        else if(rain_dou>0&&snow_dou==0)
            context.write(key,new Text("2"));
        else
            context.write(key,new Text("0"));
    }
}

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class YellowTaxiReducer extends Reducer<Text, Text, Text, Text> {
    //intermediate key value from mapper, key value reducer output 
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum_0 = 0;
        int count_0 =0;
        int sum_1 = 0;
        int count_1 =0;
        List<Integer> Rush_0_data = new ArrayList<Integer>(); //record trip_duration data
        List<Integer> Rush_1_data = new ArrayList<Integer>();
        
        for (Text value : values) {
            //key: (pickup_datetime, PULocationID, DOLocationID) -> no change
            //value: VendorID, trip_distance, trip_duration, rush_hour
            String textValue = value.toString();
            String [] parseValue = textValue.split(" ");
            String rush_hour = parseValue[3];
            if(rush_hour.equals("0")){
                int trip_duration = Integer.valueOf(parseValue[2]);
                sum_0 +=trip_duration;
                count_0 +=1;
                Rush_0_data.add(trip_duration);
            }
            if(rush_hour.equals("1")){
                int trip_duration = Integer.valueOf(parseValue[2]);
                sum_1 +=trip_duration;
                count_1 +=1;
                Rush_1_data.add(trip_duration);
            }
        }
        
        double avg_trip_duration_0 = 0;
        double avg_trip_duration_1 = 0;
        if(count_0 != 0){
            avg_trip_duration_0 = sum_0/count_0;
            
            //standard_deviation calculation
            
            double standardDeviation_0 =0;
            for(int num: Rush_0_data) {
                standardDeviation_0 += Math.pow(num - avg_trip_duration_0, 2);
            }
            
            double SD_0 = Math.sqrt(standardDeviation_0/count_0); //standard_deviation
            
            //outliner removal
            double max_threshold = avg_trip_duration_0 + 2.0*SD_0;
            double min_threshold = avg_trip_duration_0 - 2.0*SD_0;
            Iterator<Integer> i = Rush_0_data.iterator();
            while(i.hasNext()){
                int num = i.next();
                if(num > max_threshold || num < min_threshold){
                    i.remove();
                }
            }
            
            //New Avg
            int stat_sum_0 = 0;
            for(int num: Rush_0_data){
                stat_sum_0 += num;
            }
            
            int stat_count_0 =Rush_0_data.size();
            double stat_avg_trip_duration_0 = stat_sum_0/stat_count_0;
            
            context.write(new Text(key.toString() + " " + String.valueOf(stat_avg_trip_duration_0)+ " 0 " + String.valueOf(stat_count_0)), new Text(""));
        }
        if(count_1 != 0){
            avg_trip_duration_1 = sum_1/count_1;
            
            double standardDeviation_1 =0;
            for(int num: Rush_1_data) {
                standardDeviation_1 += Math.pow(num - avg_trip_duration_1, 2);
            }
            
            double SD_1 = Math.sqrt(standardDeviation_1/count_1); //standard_deviation
            
            //outliner removal
            double max_threshold = avg_trip_duration_1 + 2.0*SD_1;
            double min_threshold = avg_trip_duration_1 - 2.0*SD_1;
            Iterator<Integer> i = Rush_1_data.iterator();
            while(i.hasNext()){
                int num = i.next();
                if(num > max_threshold || num < min_threshold){
                    i.remove();
                }
            }
            
            //New Avg
            int stat_sum_1 = 0;
            for(int num: Rush_1_data){
                stat_sum_1 += num;
            }
            int stat_count_1 =Rush_1_data.size();
            double stat_avg_trip_duration_1 = stat_sum_1/stat_count_1;
            
            context.write(new Text(key.toString() + " " + String.valueOf(stat_avg_trip_duration_1)+ " 1 " + String.valueOf(stat_count_1)), new Text(""));
        }
    }
}

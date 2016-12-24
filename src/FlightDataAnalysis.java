import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDataAnalysis {

    public static class FlightScheduleMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final int delayThreshold = 10;
            String line = value.toString();
            String[] tokens = line.split(","); 
            String year = tokens[0];
            String carrier = tokens[8];
            String arrDelay = tokens[14];
            String depDelay = tokens[15];
            //if arrDelay + depDelay under delayThreshold, flight is on schedule, write 1, else flight is delayed, write 0
      	    if(!year.equals("NA") && !year.equals("Year") &&
                      !carrier.equals("NA") && !carrier.equals("UniqueCarrier") &&
                      !arrDelay.equals("NA") && !arrDelay.equals("ArrDelay") &&
                      !depDelay.equals("NA") && !depDelay.equals("DepDelay") )
            {
                if ( (Integer.parseInt(arrDelay) + Integer.parseInt(depDelay) ) <= delayThreshold)
                    context.write(new Text(carrier), new IntWritable(1));
                else
                    context.write(new Text(carrier), new IntWritable(0));
            }
      }
    }

    public static class FlightScheduleReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        List<OnSchdProb> list = new ArrayList<>();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
          int totalCount = 0;
          int onSchedule = 0;
          String ArrDepDelay;
          for (IntWritable val : values) {
              totalCount += 1;
              if(val.get() == 1)
              {
                onSchedule += 1;
              }
          }
          double prob = (double)onSchedule/(double)totalCount;
          list.add(new OnSchdProb(prob,key.toString()));
       }

       //Class to store on schedule probability of a carrier
       class OnSchdProb {
           double probability;
           String carrier;
           OnSchdProb(double prob, String carr) 
           {
                this.probability = prob;
                this.carrier = carr;
           }
       }

       // Comparator class for reverse sorting the probabilities of carriers
       class ReverseSort implements Comparator<OnSchdProb> {
            @Override
            public int compare(OnSchdProb o1, OnSchdProb o2) {
              if( o1.probability > o2.probability )
                return -1;
              else if( o1.probability < o2.probability )
                return 1;
              else 
                return 0;
            }
       }

       //Afer reducer is done, write carriers on schedule prob. in reverse order
       protected void cleanup(Context context) throws IOException, InterruptedException {
          Collections.sort(list, new ReverseSort());
          for (OnSchdProb elem : list) {
              context.write(new Text(elem.carrier), new DoubleWritable(elem.probability ));
          }
      }
    }

    public static class AirportTaxiTimeMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            String origin = tokens[16];
            String dest = tokens[17];
            String taxiIn = tokens[19];
            String taxiOut = tokens[20];
            //Emit (origin + TaxiOut) & (dest + TaxiIn) for each flight
            if(!origin.equals("NA") && !origin.equals("Origin") &&
                      !dest.equals("NA") && !dest.equals("Dest") &&
                      !taxiIn.equals("NA") && !taxiIn.equals("TaxiIn") &&
                      !taxiOut.equals("NA") && !taxiOut.equals("TaxiOut") )
            {
                int in = Integer.parseInt(taxiIn);
                int out = Integer.parseInt(taxiOut);
                context.write(new Text(origin), new IntWritable(out));
                context.write(new Text(dest), new IntWritable(in));
            }
      }
    }

    public static class AirportTaxiTimeReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        List<AvgTaxiTime> list = new ArrayList<>();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
          int totalCount = 0;
          int taxiTime = 0;
          for (IntWritable val : values) {
              totalCount += 1;
              taxiTime += val.get();
          }
          double avg = (double)taxiTime/(double)totalCount;
          list.add(new AvgTaxiTime(avg,key.toString()));
       }

       //Class to store average taxi time of an airport 
       class AvgTaxiTime {
           double avg;
           String airport;
           AvgTaxiTime(double avg, String airport)
           {
                this.avg = avg;
                this.airport = airport;
           }
       }

       // Comparator class for reverse sorting the average taxi time of airports 
       class ReverseSort implements Comparator<AvgTaxiTime> {
            @Override
            public int compare(AvgTaxiTime o1, AvgTaxiTime o2) {
              if( o1.avg > o2.avg )
                return -1;
              else if( o1.avg < o2.avg )
                return 1;
              else
                return 0;
            }
       }

       //Afer reducer is done, write airport's average taxi times in reverse order
       protected void cleanup(Context context) throws IOException, InterruptedException {
          Collections.sort(list, new ReverseSort());
          for (AvgTaxiTime elem : list) {
              context.write(new Text(elem.airport), new DoubleWritable(elem.avg ));
          }
      }
    }

    public static class FlightCancellationMapper extends Mapper<Object, Text, Text, IntWritable>{
        IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            String cancel = tokens[21];
            String code = tokens[22];
            //Emit Cancellation Code if the flight is cancelled 
            if(!cancel.equals("NA") && !cancel.equals("Cancelled") && cancel.equals("1") &&
                      !code.equals("NA") && !code.equals("CancellationCode") && !code.isEmpty())
            {
                context.write(new Text(code), one);
            }
      }
    }

    public static class FlightCancellationReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
          int totalCount = 0;
          for (IntWritable val : values) {
              totalCount += val.get();
          }
          context.write(new Text(key), new IntWritable(totalCount));
       }
    }

}

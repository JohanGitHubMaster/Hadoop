package org.mbds;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Driver class (implements the main method).
public class FusionCsv {
	
	    // Mapper class.
    public static class FusionCsvMap extends Mapper<LongWritable, Text, Text, Text> {

        public Text groupBy = new Text();
        public Text resultMap = new Text();
        public DoubleWritable profit = new DoubleWritable();
        public Text region = new Text();
        String headerString = "";
        String[] rowheaderArray;
        
        @Override
        protected void map(LongWritable lineNumber, Text row, Context context) throws IOException, InterruptedException {
            // We skip the first header line of the csv file.
            String[] header = new String[row.toString().split(",").length];
            
            String result="";
            if(lineNumber.get() == 0){
             rowheaderArray = row.toString().split(",");
             
                for(int i=0; i<row.toString().split(",").length; i++){                    
                    header[i] = rowheaderArray[i];
//                    if(i==row.toString().split(",").length-1)
                        headerString+= rowheaderArray[i];
//                    else
//                    headerString+= rowheaderArray[i]+",";
                }
                   //resultMap.set(headerString);                     
            }           
            if (lineNumber.get() != 0){
            try{               
                String[] rowArray = row.toString().split(",");
                int indexid = Arrays.asList(rowheaderArray).indexOf("");
                int indexMarque = Arrays.asList(rowheaderArray).indexOf("Marque / Modele");
                int indexMarqueCatalogue = Arrays.asList(rowheaderArray).indexOf("marque");
                int indexNomCatalogue = Arrays.asList(rowheaderArray).indexOf("nom");
                
                int indexBonus = Arrays.asList(rowheaderArray).indexOf("Bonus / Malus");
                int indexRejetCO2 = Arrays.asList(rowheaderArray).indexOf("Rejets CO2 g/km");
                int indexCoutEnerie = Arrays.asList(rowheaderArray).indexOf("Cout enerie");
                
                int indexpuissance = Arrays.asList(rowheaderArray).indexOf("puissance");
                int indexlongueur = Arrays.asList(rowheaderArray).indexOf("longueur");
                int indexnbPlaces = Arrays.asList(rowheaderArray).indexOf("nbPlaces");
                int indexnbPortes = Arrays.asList(rowheaderArray).indexOf("nbPortes");
                int indexcouleur = Arrays.asList(rowheaderArray).indexOf("couleur");
                int indexoccasion = Arrays.asList(rowheaderArray).indexOf("occasion");
                int indexprix = Arrays.asList(rowheaderArray).indexOf("prix");
                                
                if(indexid!=-1)result+=rowArray[indexid]+",";
            else result+="Null,";

                 if((indexMarqueCatalogue!=-1 && indexNomCatalogue!=-1))                     
                    result+=rowArray[indexMarqueCatalogue]+" "+rowArray[indexNomCatalogue]+",";
                 else if(indexMarque!=-1)
                     result+=rowArray[indexMarque]+",";
                 else result+="Null,";

                 
                    
              if(indexBonus!=-1)result+=rowArray[indexBonus]+",";
            else result+="Null,";
                 
               if(indexRejetCO2!=-1)result+=rowArray[indexRejetCO2]+",";
              else result+="Null,";
             
               if(indexpuissance!=-1)result+=rowArray[indexpuissance]+",";
                    else result+="Null,";
               
               if(indexlongueur!=-1)result+=rowArray[indexlongueur]+",";
                    else result+="Null,";
               
                if(indexnbPlaces!=-1)result+=rowArray[indexnbPlaces]+",";
                    else result+="Null,";
                
                if(indexnbPortes!=-1)result+=rowArray[indexnbPortes]+",";
                   else result+="Null,";
                 
                if(indexcouleur!=-1)result+=rowArray[indexcouleur]+",";
                  else result+="Null,";
                
                if(indexoccasion!=-1)result+=rowArray[indexoccasion]+",";
                  else result+="Null,";
                
                if(indexprix!=-1)result+=rowArray[indexprix];
                  else result+="Null";

                resultMap.set(result);
                //resultMap.set(rowArray[indexRejetCO2]);
                groupBy.set(rowArray[indexMarque].split(" ")[0]);
                
                context.write(groupBy, resultMap);
                }catch(Exception ex){
                }
               
            }
            
        }
    }

    // Reducer class.
    public static class FusionCsvReduce extends Reducer<Text, Text, Text, Text> {
        public Text total = new Text();
        @Override
        protected void reduce(Text group, Iterable<Text> profit, Context context) throws IOException, InterruptedException {
            String totalprofit = "";
            String result = "";
            //int result = 0;
            int count = 0;
            for (Text value : profit) {
                
                try{
                    result += value+ " \n";
                    //result += Integer.parseInt(value+"");
                }
                catch(Exception e){
                    //result += 0;
                    result+= e.getMessage()+" \n";
                }
                totalprofit +="debut "+" "+ value + " fin \n";  
                //break;
                count++;
            }
           
            //total.set(totalprofit);
            total.set(result);
            //total.set(Integer.toString(result/count));
            context.write(group, total);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Instantiate the Hadoop Configuration.
        Configuration conf = new Configuration();

        // Parse command-line arguments.
        // The GenericOptionParser takes care of Hadoop-specific arguments.
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // Check input arguments.
        if (ourArgs.length != 3) {
            System.err.println("Usage: sales <in> <out> <analyseType>");
            System.exit(2);
        }
        // We save the content of the third argument into the Hadoop Configuration object.
        //conf.set("org.mbds.FusionCsv", ourArgs[2]);

        Job job = Job.getInstance(conf, "Sales");
        // Setup the Driver/Mapper/Reducer classes.
        job.setJarByClass(FusionCsv.class);
        job.setMapperClass(FusionCsvMap.class);
        job.setReducerClass(FusionCsvReduce.class);
        // Indicate the key/value output types we are using in our Mapper & Reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Indicate from where to read input data from HDFS.
        FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
        FileInputFormat.addInputPath(job, new Path(ourArgs[1]));
        
        // Indicate from where to read input data from HDFS.
        //FileInputFormat.addInputPath(job, new Path(ourArgs[1]));-
        
        // Indicate where to write the results on HDFS.
        FileOutputFormat.setOutputPath(job, new Path(ourArgs[2]));

        // We start the MapReduce Job execution (synchronous approach).
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

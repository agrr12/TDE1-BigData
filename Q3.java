package TDE;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Locale;


public class Q3 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\in\\transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\output\\q3-resultado.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "FTYCount");

        // registro das classes
        j.setJarByClass(Q3.class);
        j.setMapperClass(Q3.MapFTYCount.class);
        j.setReducerClass(Q3.ReduceFTYCount.class);
        //Combiner
        j.setCombinerClass(Q3.CombinerFTYCount.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(FlowTypeYearWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

     public static class MapFTYCount extends Mapper<LongWritable, Text, FlowTypeYearWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            String campos[]=linha.split(";");
            if(!campos[1].equals("year")){//Verificação para pular a primeira linha
                con.write(new FlowTypeYearWritable(campos[4], campos[1]),new IntWritable(1));
            }
        }
    }
    //Combiner para facilitar o processo
    public static class CombinerFTYCount extends Reducer<FlowTypeYearWritable, IntWritable, FlowTypeYearWritable, IntWritable> {

        public void reduce(FlowTypeYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable v : values){
                sum += v.get(); // get retorna o valor em int tradicional
            }
            con.write(key, new IntWritable(sum));

        }
    }

    public static class ReduceFTYCount extends Reducer<FlowTypeYearWritable, IntWritable, Text, IntWritable> {

        public void reduce(FlowTypeYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable v : values){
                sum += v.get(); // get retorna o valor em int tradicional
            }
            con.write(new Text(key.toString()), new IntWritable(sum));


        }
    }

}

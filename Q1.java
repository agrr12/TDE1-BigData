package TDE;

import java.io.IOException;
import java.util.Locale;

import basic.Teste;
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


public class Q1 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\in\\transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\output\\q1-resultado.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "BrazilCount");

        // registro das classes
        j.setJarByClass(Q1.class);
        j.setMapperClass(Q1.MapBrazilCount.class);
        j.setReducerClass(Q1.ReduceBrazilCount.class);
        //Combiner
        j.setCombinerClass(ReduceBrazilCount.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);



        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapBrazilCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            String campos[]=linha.split(";");
            if(!campos[1].equals("year")){ //Verificação para pular a 1a linha
                if (campos[0].equals("Brazil")) { //Joga para o reduce apenas as transações que envolvem o Brasil
                    con.write(new Text(campos[0]), new IntWritable(1));
                }
            }
        }
    }



    public static class ReduceBrazilCount extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0; //Variável que agrega o número de vezes que Brasil aparece em transações
            for (IntWritable v : values){
                sum += v.get(); // get retorna o valor em int tradicional
            }
            con.write(key, new IntWritable(sum));


        }
    }

}

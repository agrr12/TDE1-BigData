package TDE;

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
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class Q6 {



    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\in\\transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\output\\q6-resultado.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "AvgCommodityUTypeYearFlow");

        // registro das classes
        j.setJarByClass(Q6.class);
        j.setMapperClass(Q6.MapUnityTypeYear.class);
        j.setReducerClass(Q6.ReduceUnityTypeYear.class);
        //Combiner
        //j.setCombinerClass(Q5.ReduceAvgCommodityUTypeYearFlow.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(unitTypeYearWritable.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapUnityTypeYear extends Mapper<LongWritable, Text, unitTypeYearWritable, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String campos[]=linha.split(";");
            if(!campos[1].equals("year")){//Verificação para pular a primeira linha
                con.write(new unitTypeYearWritable(campos[7],campos[1]),new DoubleWritable(Double.parseDouble(campos[8])));
            }
        }
    }

    public static class ReduceUnityTypeYear extends Reducer<unitTypeYearWritable, DoubleWritable, Text, Text> {

        public void reduce(unitTypeYearWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double MAX = Double.MIN_VALUE;
            double MIN = Double.MAX_VALUE;
            double somaTotal = 0.0;
            double qtdValores = 0.0;

            for (DoubleWritable v : values){
                qtdValores+=1.0;
                double valor = v.get();
                somaTotal += valor;
                if(valor>MAX){MAX=valor;}
                if(valor<MIN){MIN=valor;}
            }
            //Monta uma STRING para o retorno dos valores desejados
            String retorno = "MAX:"+ Double.toString(MAX) +" | MIN: "+ Double.toString(MIN)+" | MEDIA:" +Double.toString(somaTotal/qtdValores);
            con.write(new Text(key.toString()), new Text(retorno));

        }
    }

}

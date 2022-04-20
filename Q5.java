package TDE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class Q5 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\in\\transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\output\\q5-resultado.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "AvgCommodityUTypeYearFlow");

        // registro das classes
        j.setJarByClass(Q5.class);
        j.setMapperClass(Q5.MapAvgCommodityUTypeYearFlow.class);
        j.setReducerClass(Q5.ReduceAvgCommodityUTypeYearFlow.class);
        //Combiner
        j.setCombinerClass(Q5.ReduceAvgCommodityUTypeYearFlow.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(unitTypeYearWritable.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapAvgCommodityUTypeYearFlow extends Mapper<LongWritable, Text, unitTypeYearWritable, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            String campos[]=linha.split(";");
            //Verificação para pular a 1a linha e jogar para o reduce apenas casos referentes ao Brasil e a Exportações
            //Como trata-se apenas dos casos de EXPORT, assume-se ser desnecessário utilizar EXPORT como chave
            if(!campos[1].equals("year") && campos[0].equals("Brazil") && campos[4].equals("Export")){
                con.write(new unitTypeYearWritable(campos[7],campos[1]),new DoubleWritable(Double.parseDouble(campos[5])));
            }
        }
    }

    public static class ReduceAvgCommodityUTypeYearFlow extends Reducer<unitTypeYearWritable, DoubleWritable, Text, DoubleWritable> {


        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double qtdValores = 0.0; //Armazena quantos valores estão associados a uma única chave
            double somaTotal = 0.0; //Armazena a soma de todos os valores associados a uma única chave

            for (DoubleWritable v : values){
                qtdValores+=1.0;
                somaTotal += v.get();
            }
            con.write(new Text(key.toString()), new DoubleWritable(somaTotal/qtdValores));

        }
    }

}

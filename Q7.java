package TDE;

import advanced.entropy.BaseQtdWritable;
import advanced.entropy.EntropyFASTA;
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


public class Q7 {



    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\in\\transactions_amostra.csv");

        Path intermediate = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\output\\q7-inter.txt");

        // arquivo de saida
        Path output = new Path("C:\\Users\\hadoop\\Desktop\\fasf\\big-data-mapreduce-hadoop3-student-master\\output\\q7-resultado.txt");

        // criacao do job e seu nome
        Job j1 = new Job(c, "MostCommodity2016PerFlowType");

        // registro das classes
        j1.setJarByClass(Q7.class);
        j1.setMapperClass(Q7.Map1.class);
        j1.setReducerClass(Q7.Reduce1.class);
        //Combiner
        //j1.setCombinerClass(Q5.ReduceAvgCommodityUTypeYearFlow.class);

        // definicao dos tipos de saida
        j1.setMapOutputKeyClass(FlowTypeCommodityWritable.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // lanca o job e aguarda sua execucao
        if(!j1.waitForCompletion(true)){
            System.err.println("Error with Job 1!");
            return;
        }

        Job j2 = new Job(c, "fasta-entropy-pt2");

        // registro das classes
        j2.setJarByClass(Q7.class);
        j2.setMapperClass(Q7.Map2.class);
        j2.setReducerClass(Q7.Reduce2.class);

        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CommodityQuantityWritable.class);
        j2.setOutputKeyClass(CommodityQuantityWritable.class);
        j2.setOutputValueClass(Text.class);

        // arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        if(!j2.waitForCompletion(true)){
            System.err.println("Error with Job 2!");
            return;
        }
    }

    //INÍCIO DA PRIMEIRA ROTINA
    //OBTER CHAVE [FLOWTYPE, COMMODITY] E VALOR [AMOUNT]

    public static class Map1 extends Mapper<LongWritable, Text, FlowTypeCommodityWritable, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String campos[]=linha.split(";");
            if(campos[1].equals("2016")){//Seleciona apenas casos de 2016
                con.write(new FlowTypeCommodityWritable(campos[4], campos[3]), new DoubleWritable(Double.parseDouble(campos[8])));
            }
        }
    }

    public static class Reduce1 extends Reducer<FlowTypeCommodityWritable, DoubleWritable, Text, DoubleWritable> {

        public void reduce(FlowTypeCommodityWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double total = 0.0;

            for (DoubleWritable v : values){
                total+= v.get();
            }
            con.write(new Text(key.toString()), new DoubleWritable(total));

        }
    }

    //INÍCIO DA SEGUNDA ROTINA
    //OBTER CHAVE [FLOWTYPE] E VALOR [COMMODITY MAIS EXPORTADA]
    //RECEBE ARQUIVO COM CADA LINHA NO FORMATO "COMMODITY;FLOWTYPE  AMOUNT"

    public static class Map2 extends Mapper<LongWritable, Text, Text, CommodityQuantityWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            String campos[]=linha.split("\t"); //"COMMODITY;FLOWTYPE  AMOUNT" -> ["COMMODITY;FLOWTYPE", "AMOUNT"]
            String quantidade = campos[1];

            String[] camposCF = campos[0].split(";"); //"COMMODITY;FLOWTYPE" -> ["COMMODITY", "FLOWTYPE"]
            String commodity = camposCF[0];
            String flowType = camposCF[1];
            con.write(new Text(flowType), new CommodityQuantityWritable(commodity, quantidade));
        }
    }

    public static class Reduce2 extends Reducer<Text, CommodityQuantityWritable, Text, Text> {

        public void reduce(Text key, Iterable<CommodityQuantityWritable> values, Context con)
                throws IOException, InterruptedException {
            String maiorOcorrencia = ""; //Armazena o nome da Commodity mais comercializada
            double quantidadeOcorrencia = 0.0; //Armazena a quantidade de vezes que uma determinada commodity aparece por flow type

            for (CommodityQuantityWritable v : values){
                if(Double.parseDouble(v.getQuantity())>quantidadeOcorrencia){
                    maiorOcorrencia = v.getCommodity();
                    quantidadeOcorrencia = Double.parseDouble(v.getQuantity());
                }

            }
            con.write(new Text(key), new Text(maiorOcorrencia+"| Quantidade Total: "+String.valueOf(quantidadeOcorrencia)));

        }
    }

}

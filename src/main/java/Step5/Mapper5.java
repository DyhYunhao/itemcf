package Step5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> cacheList = new ArrayList<String>();

    private DecimalFormat df = new DecimalFormat("0.00");


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //输入流读入矩阵
        FileReader fr = new FileReader("itemUserScore3");
        BufferedReader br = new BufferedReader(fr);

        String line = null;
        while ((line = br.readLine()) != null){
            cacheList.add(line);
        }

        fr.close();
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String item_matrix1 = value.toString().split("\t")[0];
        String[] user_score_array_matrix1 = value.toString().split("\t")[1].split(",");

        for (String line: cacheList){
            String item_matrix2 = line.toString().split("\t")[0];
            String[] user_score_array_matrix2 = line.toString().split("\t")[1].split(",");

            //如果物品id相同
            if (item_matrix1.equals(item_matrix2)){
                for (String user_score_matrix1: user_score_array_matrix1) {
                    String use_matrix1 = user_score_matrix1.split("_")[0];
                    String score_matrix1 = user_score_matrix1.split("_")[1];

                    boolean flag = false;
                    for (String user_score_matrix2: user_score_array_matrix2) {
                        String use_matrix2 = user_score_matrix2.split("_")[0];
                        if (use_matrix1.equals(use_matrix2)){
                            flag = true;
                        }
                    }
                    if (!flag){
                        outKey.set(use_matrix1);
                        outValue.set(item_matrix1 + "_" + score_matrix1);
                        context.write(outKey, outValue);
                    }
                }
            }

        }
    }
}

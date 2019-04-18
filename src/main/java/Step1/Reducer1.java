package Step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String itemId = key.toString();

        //<userId, score >
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Text value: values){
            String userId = value.toString().split("_")[0];
            String score = value.toString().split("_")[1];

            if (map.get(userId) == null){
                map.put(userId, Integer.valueOf(score));
            }else {
                Integer preScore = map.get(userId);
                map.put(userId, preScore + Integer.valueOf(score));
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry: map.entrySet()){
            String userId = entry.getKey();
            String score = String.valueOf(entry.getValue());
            sb.append(userId).append("_").append(score).append(",");
        }
        //去掉行末的,
        String line = null;
        if (sb.toString().endsWith(",")){
            line = sb.substring(0, sb.length() - 1);
        }

        outKey.set(itemId);
        outValue.set(line);

        context.write(outKey, outValue);

    }
}


import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class URLFileGenerator {
    static public void main(String[] args) throws IOException {
        int partitions = 4;
        int urlLength = 50;
        int recordCntPerPartition = 1000*30*1000;
        Random rand = new Random();
        for(int p = 0;p<partitions;p++){
            FileWriter fileWriter = new FileWriter(new File("./dataset//"+p));
            int cnt = 0;
            while(cnt < recordCntPerPartition){
                //Random Twice to increase cardinality
                fileWriter.write("http://"+
                        StringUtils.repeat(Util.alphabets[rand.nextInt(26)],1+rand.nextInt(urlLength/2))+
                        StringUtils.repeat(Util.alphabets[rand.nextInt(26)],1+rand.nextInt(urlLength/2))+
                        "\n");
                cnt += 1;
            }
            fileWriter.flush();
            fileWriter.close();
        }

    }


}

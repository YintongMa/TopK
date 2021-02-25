import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//not thread safe
public class PartitionParallelReader {

    List<BufferedReader> readers = new ArrayList<>();
    int bufferSize = 1024;
    List<String> partitions;
    Pair<String, Long>[] curkv ;

    public PartitionParallelReader(List<String> partitions, int bufferSize){
        this.partitions = partitions;
        this.curkv = new Pair[partitions.size()];
        this.bufferSize = bufferSize;
    }

    public void open() throws IOException {
        try {

            for(int i = 0;i<partitions.size();i++){
                this.readers.add(new BufferedReader(new FileReader(partitions.get(i)), bufferSize));
                next(i);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }



    }

    private boolean next(int i) throws IOException {
        String line = readers.get(i).readLine();
        if (line != null){
            String[] kv = line.split(",");
            this.curkv[i] = new Pair<String, Long>(kv[0],Long.parseLong(kv[1]));
            return true;
        }else{
            this.curkv[i] = null;
            return false;
        }
    }


    public Pair<String, Long> nextMinKV()  throws IOException {
        String minkey = null;
        //todo optimize double loop
        Long value = 0l;
        for(int i = 0;i<partitions.size();i++){
            if(curkv[i] != null && (minkey == null || curkv[i].getKey().compareTo(minkey) < 0)){
                minkey = curkv[i].getKey();
            }
        }

        for(int i = 0;i<partitions.size();i++){
            if (curkv[i] != null && curkv[i].getKey().compareTo(minkey) == 0){
                value += curkv[i].getValue();
                next(i);
            }
        }

        if(minkey != null){
            return new Pair<String, Long>(minkey,value);
        }

        return null;

    }

}

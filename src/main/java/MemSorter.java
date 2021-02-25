
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MemSorter implements Runnable{
    int id;
    List<String> bars ;
    PartitionCascadeReader reader;
    int mergerParallelism = 4;
    int memSize; //mb
    int urlLength; //byte cnt
    int recordLimit; //cnt
    String tempFilePath;
    public MemSorter(int id, PartitionCascadeReader reader, List<String> bars, int memSize, int urlLength, int mergerParallelism, String tempFilePath){
        this.id = id;
        this.reader = reader;
        this.bars = bars;
        this.memSize = memSize;
        this.urlLength = urlLength;
        this.mergerParallelism = mergerParallelism;
        this.recordLimit = memSize*1024*1024/(urlLength + 8);
        this.tempFilePath = tempFilePath;
    }

    public void buildSortedBlocks() throws IOException {
        Map<String, Long> map = new TreeMap();
        int batchID = 0;

        reader.open();
        while(reader.next()){
            String url = reader.cur();
            if(map.containsKey(url)){
                map.put(url, map.get(url)+1);
            }else{
                map.put(url, 1l);
            }
            if (map.size() >= recordLimit){
                eviction(map, batchID);
                map = new TreeMap();
                batchID += 1;
            }

        }
        eviction(map, batchID);

    }

    private void eviction(Map<String, Long> map, int batchID) throws IOException {
        List<FileWriter> fileWriters = new ArrayList<>();
        for(int i = 0;i < mergerParallelism; i++){
            try {
                fileWriters.add(new FileWriter(new File(tempFilePath+"temp_"+i+"_"+id+"_"+batchID)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int partition = 0;
        for (String key:map.keySet()){
            String line = key+","+map.get(key)+"\n";
            while(partition < mergerParallelism - 1 && key.compareTo(bars.get(partition)) > 0){
                partition += 1;
            }
            fileWriters.get(partition).write(line);
        }

        for (FileWriter fileWriter :fileWriters){
            fileWriter.flush();
            fileWriter.close();
        }

    }


    @Override
    public void run() {
        try {
            buildSortedBlocks();
            System.out.println("MemSorter finished, id "+id);
        } catch (IOException e) {
            System.out.println("MemSorter error: "+e);
            e.printStackTrace();
        }
    }
}

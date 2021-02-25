import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.*;

public class TopkURL {

    static String tempFilePrefix = "temp_";
    static String tempFilePath = "./topk_temp/";
    static String datasetPath = "./dataset";

    static public void main(String[] args) throws InterruptedException, IOException {

        //params
        int k = 10;
        int sortParallelism = 4;
        int mergerParallelism = 4;
        int memSize = 40; //mb
        int url_length = 50;

        k = Integer.parseInt(System.getProperty("k"));
        sortParallelism = Integer.parseInt(System.getProperty("sortParallelism"));
        mergerParallelism = Integer.parseInt(System.getProperty("mergerParallelism"));
        memSize = Integer.parseInt(System.getProperty("memSize"));
        url_length = Integer.parseInt(System.getProperty("url_length"));
        datasetPath = System.getProperty("dataset");

        ExecutorService sorterPool = Executors.newFixedThreadPool(sortParallelism);
        ExecutorService mergerPool = Executors.newFixedThreadPool(mergerParallelism);

        //Stage 0 - Preparation
        File directory = new File(tempFilePath);
        if (! directory.exists()){
            directory.mkdir();
        }
        List<String> urlDistributionBars = Util.getUrlDistributionBars(mergerParallelism);

        //Stage 1 - Sort
        List<List<String>> inputFileByPartition = Util.getFilesNamesByPartition(sortParallelism,"",datasetPath);
        for(int i = 0;i < sortParallelism; i++){
            sorterPool.submit(
                    new MemSorter(
                            i,
                            new PartitionCascadeReader(inputFileByPartition.get(i)),
                            urlDistributionBars,
                            memSize/sortParallelism, url_length, mergerParallelism,tempFilePath
                    )
            );
        }
        sorterPool.shutdown();
        sorterPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        //Stage 2 - Merge
        List<List<String>> filesNamesByPartition = Util.getFilesNamesByPartition(mergerParallelism,tempFilePrefix,tempFilePath);
        DiskMerger[] mergers = new DiskMerger[mergerParallelism];
        for(int i = 0;i < mergerParallelism; i++){
            List<String> partitions = filesNamesByPartition.get(i);
            mergers[i] = new DiskMerger(i,new PartitionParallelReader(partitions,memSize*1024/mergerParallelism/partitions.size()/2), k);
            mergerPool.submit(mergers[i]);
        }
        mergerPool.shutdown();
        mergerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        //Stage 3 - Collect
        List<PriorityQueue<KVPair<String, Long>>> minHeaps = new ArrayList<>();
        for(DiskMerger merger: mergers){
            minHeaps.add(merger.getMinheap());
        }
        PriorityQueue<KVPair<String, Long>> topk = Util.mergeMinHeaps(minHeaps,k);

        System.out.println("TopK Result Desc:");
        while (!topk.isEmpty()){
            System.out.println(topk.poll());
        }

        //clean up
        FileUtils.deleteDirectory(new File(tempFilePath));

    }
}



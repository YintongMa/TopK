import javafx.util.Pair;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class Util {

    static String[] alphabets = new String[]{"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
    static public PriorityQueue<Pair<String, Long>> mergeMinHeaps(List<PriorityQueue<Pair<String, Long>>> minHeaps, int k){
        PriorityQueue<Pair<String, Long>> minheap = minHeaps.get(0);

        for(int i=1;i<minHeaps.size();i++){
            PriorityQueue<Pair<String, Long>> heap = minHeaps.get(i);
            while (heap.size() != 0){
                Pair<String, Long> pair = heap.poll();
                if (minheap.size() < k){
                    minheap.add(pair);
                }else{
                    if(pair.getValue() > minheap.peek().getValue()){
                        minheap.poll();
                        minheap.add(pair);
                    }
                }
            }

        }

        return minheap;
    }

    public static ArrayList<String> getFiles(String path) {
        ArrayList<String> files = new ArrayList<String>();
        File file = new File(path);
        File[] tempList = file.listFiles();
        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                files.add(tempList[i].toString());
            }
            if (tempList[i].isDirectory()) {
            }
        }
        return files;
    }


    public static List<List<String>> getFilesNamesByPartition(int totalPartitions, String prefix, String path) {
        List<List<String>> filesNamesByPartition = new ArrayList<>();
        for (int i=0;i<totalPartitions;i++){
            filesNamesByPartition.add(new ArrayList<>());
        }
        File file = new File(path);
        File[] tempList = file.listFiles();
        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                String fileName = tempList[i].toString();
                System.out.println(FilenameUtils.getName(fileName));
                int p = Integer.parseInt(FilenameUtils.getName(fileName).substring(prefix.length(),prefix.length()+1)) % totalPartitions;
                System.out.println("p:"+p);
                filesNamesByPartition.get(p).add(fileName);
            }
            if (tempList[i].isDirectory()) {
            }
        }
        return filesNamesByPartition;
    }

    public static List<String> getUrlDistributionBars(int totalPartitions) {
        assert totalPartitions <= 27;

        List<String> bars = new ArrayList<>();
        int step = 27 / totalPartitions;
        for (int i=1;i<totalPartitions;i++){
            bars.add("http://"+alphabets[step*i - 1]);
        }
        return bars;
    }

    //todo
    public static List<String> sampleUrlDistributionBars(int totalPartitions, int sampleRate, String path) {
        return null;
    }

    static public void main(String[] args){
        for(String s :getUrlDistributionBars(4)){
            System.out.println(s);
        }
    }
}

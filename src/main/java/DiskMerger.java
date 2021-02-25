import javafx.util.Pair;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class DiskMerger implements Runnable{

    int k = 10;

    PartitionParallelReader reader;

    PriorityQueue<Pair<String, Long>> minheap = new PriorityQueue<Pair<String, Long>>(new Comparator<Pair<String, Long>>() {
        @Override
        public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
            return (int) (o1.getValue() - o2.getValue());
        }
    });

    public DiskMerger(PartitionParallelReader reader, int k) {
        this.reader = reader;
        this.k = k;
    }


    public void merge() throws IOException {

        reader.open();

        Pair<String, Long> minkv = reader.nextMinKV();

        while(minkv != null){
//            System.out.println(minkv);
            if (minheap.size() < k){
                minheap.add(minkv);
            }else{
                if(minkv.getValue() > minheap.peek().getValue()){
                    minheap.poll();
                    minheap.add(minkv);
                }
            }

            minkv = reader.nextMinKV();

        }

    }

    public PriorityQueue<Pair<String, Long>> getMinheap(){
        return this.minheap;
    }

    static public void main(String[] args) throws IOException {
//        DiskMerger merger = new DiskMerger(new PartitionParallelReader(new String[]{"temp_0_0_0","temp_1_0_0","temp_2_0_0","temp_3_0_0"},"/Users/mayintong/Workspace/TopK"), 10);
//        merger.merge();
//        while(merger.getMinheap().size() != 0){
//            System.out.println(merger.getMinheap().poll());
//        }

    }

    @Override
    public void run() {
        try {
            merge();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

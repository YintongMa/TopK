import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class DiskMerger implements Runnable{

    int k = 10;
    int id;
    PartitionParallelReader reader;

    PriorityQueue<KVPair<String, Long>> minheap = new PriorityQueue<KVPair<String, Long>>(new Comparator<KVPair<String, Long>>() {
        @Override
        public int compare(KVPair<String, Long> o1, KVPair<String, Long> o2) {
            return (int) (o1.getValue() - o2.getValue());
        }
    });

    public DiskMerger(int id, PartitionParallelReader reader, int k) {
        this.reader = reader;
        this.k = k;
        this.id = id;

    }


    public void merge() throws IOException {
        reader.open();

        KVPair<String, Long> minkv = reader.nextMinKV();

        while(minkv != null){
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

    public PriorityQueue<KVPair<String, Long>> getMinheap(){
        return this.minheap;
    }


    @Override
    public void run() {
        try {
            merge();
            System.out.println("DiskMerger finished, id "+id);
        } catch (Exception e) {
            System.out.println("DiskMerger error: "+e);
            e.printStackTrace();
        }
    }
}

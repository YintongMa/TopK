import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//not thread safe
public class PartitionCascadeReader {

    List<BufferedReader> readers = new ArrayList<>();
    int readerIndex = 0;
    int bufferSize = 1024;
    List<String> partitions;
    String cur;

    public PartitionCascadeReader(List<String> partitions){
        this.partitions = partitions;
    }

    public void open() throws FileNotFoundException {
        for(String partition: partitions){
            readers.add(new BufferedReader(new FileReader(partition), bufferSize));
        }
    }

    public boolean next(){
        try {
            cur = readers.get(readerIndex).readLine();
            if (cur == null){
                if (readerIndex == readers.size() - 1){
                    return false;
                }
                readerIndex += 1;
                return next();

            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    public String cur(){
        return cur;
    }

}

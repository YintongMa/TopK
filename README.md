# TopK URL

There is a 100 GB file of URLs on the disk. Please write a small project to calculate the top 100 most common URLs and their frequency respectively in the file within 1 GB of memory, the faster the better.

## Design

Designed and implemented a variation of parallel external sorting to find TopK elements with O(M * N log R + N / R * D * 2) cost. 

(M is one in-memory comparision cost; D is one-batch write to disk cost; R is the maximum record cnt in memory; N is total record cnt)

### Stage 0 - Preparation
1. Generate url distribution bars to partition the URL key space for merge workload balance.

### Stage 1 - Sort
1. Each thread parallelly sorts File partitions in memory block by block (block_size = total_mem_size/parallelism)
2. Each thread partitions the in memory sorted block by url distribution bars (to parallel Stage 2) 
3. Each thread batch-writes the memory sorted blocks to temp files on disk 
4. Loop Stage 1 until raw file ends

### Stage 2 - Merge
1. Each thread parallelly mergesorts the temp files partitioned by url distribution bars and read into memory streamly
2. Each thread maintains an in-memory TopK minheap
3. Loop until all temp files end

### Stage 3 - Collect
1. Merge each thread's TopK minheap into one final TopK minheap
2. Output the final TopK minheap

## Experiment
Because of local PC limitation, this experiment will scale down both the file size and memory limitation.

### Mock Dataset
1. Run the URLFileGenerator.class to generate 4GB files to ./dataset by default. The user could also change the params to generate larger or smaller files.

### Run TopK
1. Build TopkURL jar with build.gradle to generate TopkURL-1.0-SNAPSHOT-all.jar in ./build/libs
2. Run the folowing CMD, which will use 4 cpu cores and 40mb memory to parallelly find the top 10 URLs from 4GB url dataset in ./dataset. The maxlength of URL here is set to 50 characters.
```
java -Dk=10 -DsortParallelism=4 -DmergerParallelism=4 -DmergerParallelism=4 -DmemSize=40 -Durl_length=50 -Ddataset=./dataset -jar TopkURL-1.0-SNAPSHOT-all.jar

```
## Output Result
```
MemSorter finished, id 3
MemSorter finished, id 0
MemSorter finished, id 1
MemSorter finished, id 2
DiskMerger finished, id 0
DiskMerger finished, id 2
DiskMerger finished, id 1
DiskMerger finished, id 3
TopK Result Desc:
http://kkkkkkkkkkkkkkkkkkkkkkkkkk=7154
http://vvvvvvvvvvvvvvvvvvvvvvvvvv=7155
http://nnnnnnnnnnnnnnnnnnnnnnnnnn=7156
http://eeeeeeeeeeeeeeeeeeeeeeeeee=7183
http://oooooooooooooooooooooooooo=7183
http://bbbbbbbbbbbbbbbbbbbbbbbbbb=7217
http://aaaaaaaaaaaaaaaaaaaaaaaaaa=7218
http://ssssssssssssssssssssssssss=7222
http://pppppppppppppppppppppppppp=7248
http://xxxxxxxxxxxxxxxxxxxxxxxxxx=7284
```

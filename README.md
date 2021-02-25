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

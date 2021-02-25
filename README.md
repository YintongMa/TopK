# TopK

There is a 100 GB file of URLs on the disk. Please write a small project to calculate the top 100 most common URLs and their frequency respectively in the file within 1 GB of memory, the faster the better.

## Design

Designed and implemented a variation of parallel external sorting to find TopK elements with O(M * N log R + N / R * D * 2) cost. 

(M is one in-memory comparision cost; D is one-batch write to disk cost; R is the maximum record cnt in memory; N is total record cnt)

#ifndef	__STORAGENODE_H__
#define	__STORAGENODE_H__	1

#include "metadata.h"
#include "bitmap.h"
#include "lru.h"
#include "darc.h"
#include "cachemem.h"

class StorageNode {
public:
#define	QLEN 5
	MDMaps	*md;		// All types of deduplication related
				// Metadata structures
	BitMap		*bmap;	// Bit map for free block information
	CacheMem	*mem;
	pthread_mutex_t	lck;
	pthread_cond_t	cond;
	struct FileBlocks *fblks[QLEN];
	struct segment4k *segs[QLEN];
	int *busyptr[QLEN];
	int operation[QLEN];
	int reqcount;
	int head, tail;
	int completed;
	unsigned long long int curTime;
	unsigned char counters[8192];
	unsigned int ublocks;
	unsigned int dtype;



	//////////////////////////////////////////////////////
	unsigned long long int DATA_BLOCK_READ_CACHE_HIT;
	unsigned long long int DATA_BLOCK_READ_CACHE_HIT_LDC;
	unsigned long long int DATA_BLOCK_READ_CACHE_HIT_INDIRECT;
	unsigned long long int DATA_BLOCK_READ_CACHE_MISS;
	unsigned long long int DATA_BLOCK_READ_CACHE_MISS_LDC;
	unsigned long long int DATA_BLOCK_READ_CACHE_MISS_INDIRECT;
	unsigned long long int DATA_BLOCK_READ_COUNT;
	unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT;
	unsigned long long int DATA_BLOCK_WRITE_CACHE_MISS;
	unsigned long long int DATA_BLOCK_WRITE_COUNT;
	unsigned long long int DATA_BLOCK_EFFECTIVE_WRITES;
	unsigned long long int DATA_BLOCK_FLUSH_WRITES;
	unsigned long long int UNIQUE_BLOCK_COUNT;

	unsigned long long int DISK_READ_COUNT[2];
	unsigned long long int DISK_READ_BCOUNT[2];
	unsigned long long int DISK_WRITE_COUNT[2];
	unsigned long long int DISK_WRITE_BCOUNT[2];
	double DISK_READ_TOT_TIME[2];             // Time in mille seconds
	double DISK_WRITE_TOT_TIME[2];
	double DISK_READ_MIN_TIME[2]; // Time in mille seconds
	double DISK_READ_MAX_TIME[2];
	double DISK_WRITE_MIN_TIME[2];        // Time in mille seconds
	double DISK_WRITE_MAX_TIME[2];
	unsigned long long int lastoptime[2];
	//////////////////////////////////////////////////////

	double t1_disk0, t1_disk1, t2_disk0, t2_disk1;
	double t3_disk0, t3_disk1;
	double t1_clk, t2_clk, t3_clk, t0_hash, t0_dcomm, t0_mdcomm;
	double t1_dcomm;
	double t_cumulative;	// Including execution, metadata read/writes
	double t_rcumulative;	// Including execution, metadata read/writes
	

	double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
	unsigned long int r_n[1028], w_n[1028];
	double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
	double w_flush;

	unsigned long int CLOCK_PREV, CLOCK_MICRO;
	unsigned long int DISK_MILLE, DISK_PREV1, DISK_PREV2;
	unsigned long int prevblock[2];
	//unsigned long int NCLOCK_TIME_TOTAL;
	double SEGCOUNT3;
	double SEGLENGTH3;
	double RSEGCOUNT3;
	double RSEGLENGTH3;

	//////////////////////////////////////////////////////


#ifdef	DARCP
	DARC	*dc;		// Data cache
#else
	LRU	*dc;
#endif

	StorageNode(const char *metadata_dir, unsigned int deduptype,
		unsigned int dclist, unsigned long int dcsize, 
		unsigned int blksize);
	//void insertRequest(FileBlocks *fb, int *bptr);
	//void insertRequest(segment4k *s, int *bptr);
	//void deleteRequest(void);
	//int checkAndWait(void);
	//void setCompleted(void);
	//void processNextRequest(void);
	unsigned int estimateUniqueBlockCount(unsigned char set2counters[], int set2size);
	//void addTime(unsigned long v) { NCLOCK_TIME_TOTAL += v; }
	//unsigned long getTime(void) { return NCLOCK_TIME_TOTAL; }
};

//void *storageNodeThread(StorageNode *sn);

#endif

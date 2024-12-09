#include <string.h>
#include "storagenode.h"


void updateCounters(unsigned char counter1[], unsigned char counter2[],
                unsigned char counter3[]);
double estimateCardinality(unsigned char counter[]);

StorageNode::StorageNode(const char *metadata_dir, unsigned int deduptype, 
		unsigned int dclist, unsigned long int dcsize, 
		unsigned int blksize) {
	int i;
	char metadataFiles[9][128];
	char metadataFilesBasenames[9][30] = {"blockidx.dat", "pbaidx.dat", 
		"hashidx.dat", "filehashidx.dat", "buckets.dat", 
		"bucketidx.dat", "hashbuckets.dat", "pbaref.dat", "fblist.dat"};
	char bmapfile[128];







	reqcount=0;
	head = 0;
	tail = 0;
	completed = 0;
	ublocks = 0;

	DATA_BLOCK_READ_CACHE_HIT = 0;
	DATA_BLOCK_READ_CACHE_HIT_LDC = 0;
	DATA_BLOCK_READ_CACHE_HIT_INDIRECT = 0;
	DATA_BLOCK_READ_CACHE_MISS = 0;
	DATA_BLOCK_READ_CACHE_MISS_LDC = 0;
	DATA_BLOCK_READ_CACHE_MISS_INDIRECT = 0;
	DATA_BLOCK_READ_COUNT = 0;
	DATA_BLOCK_WRITE_CACHE_HIT = 0;
	DATA_BLOCK_WRITE_CACHE_MISS = 0;
	DATA_BLOCK_WRITE_COUNT = 0;
	DATA_BLOCK_EFFECTIVE_WRITES = 0;
	DATA_BLOCK_FLUSH_WRITES = 0;
	UNIQUE_BLOCK_COUNT = 0;

	for (i=0; i<2; i++) {
		DISK_READ_COUNT[i] = 0;
		DISK_READ_BCOUNT[i] = 0;
		DISK_WRITE_COUNT[i] = 0;
		DISK_WRITE_BCOUNT[i] = 0;
		DISK_READ_TOT_TIME[i] = 0;             // Time in mille seconds
		DISK_WRITE_TOT_TIME[i] = 0;
		DISK_READ_MIN_TIME[i] = 10000; // Time in mille seconds
		DISK_READ_MAX_TIME[i] = 0;
		DISK_WRITE_MIN_TIME[i] = 10000;        // Time in mille seconds
		DISK_WRITE_MAX_TIME[i] = 0;
		lastoptime[i] = 0 ;
		prevblock[i] = 0;
	}
	//////////////////////////////////////////////////////

	t1_disk0 = 0; t1_disk1 = 0; t2_disk0 = 0; t2_disk1 = 0;
	t3_disk0 = 0; t3_disk1 = 0;
	t1_clk = 0; t2_clk = 0; t3_clk = 0; t0_hash = 0; t0_dcomm = 0;
	t0_mdcomm = 0; t1_dcomm = 0;
	t_cumulative = 0;
	t_rcumulative = 0;

	for (i=0; i<1028; i++) {
		r_var[i] = 0; 
		r_mean[i] = 0; 
		r_min[i] = 10000; 
		r_max[i] = 0;
		r_n[i] = 0; 
		w_n[i] = 0;
		w_var[i] = 0; 
		w_mean[i] = 0; 
		w_min[i] = 10000;
		w_max[i] = 0;
	}
	w_flush=0;

	CLOCK_PREV = 0; CLOCK_MICRO = 0;
	DISK_MILLE = 0; DISK_PREV1 = 0; DISK_PREV2 = 0;

	SEGCOUNT3 = 0.0;
	SEGLENGTH3 = 0.0;
	RSEGCOUNT3 = 0.0;
	RSEGLENGTH3 = 0.0;

	mem = new CacheMem();
	mem->initCacheMem(metadata_dir);

	for (i=0; i<9; i++) {
		strcpy(metadataFiles[i], metadata_dir);
		strcat(metadataFiles[i], "/");
		strcat(metadataFiles[i], metadataFilesBasenames[i]);
	}

	md = new MDMaps(metadataFiles[0], metadataFiles[1], metadataFiles[2],
		metadataFiles[3], metadataFiles[4], metadataFiles[5], 
		metadataFiles[6], metadataFiles[7], metadataFiles[8],
		deduptype, this);

	strcpy(bmapfile, metadata_dir);
	strcat(bmapfile, "/");
	strcat(bmapfile, "bitmap.dat");
	bmap = new BitMap(bmapfile);

#ifdef	DARCP
	dc = new DARC(metadata_dir, dclist, dcsize, blksize, this);
#else
	dc = new LRU(metadata_dir, dclist, dcsize, this);
#endif

	dtype = deduptype;
	pthread_mutex_init(&lck, NULL);
	pthread_cond_init(&cond, NULL);
	for (i=0; i<QLEN; i++) {
		fblks[i] = NULL;
		segs[i] = NULL;
		busyptr[i] = NULL;
		operation[i] = 0;
	}
	head = tail = 0;
	reqcount = 0;
	completed = 0;
}

unsigned int StorageNode::estimateUniqueBlockCount(unsigned char set2counters[], int set2size) {
	double s1, s2;
	double union_size;
	unsigned char ucounters[8192];

	s1 = estimateCardinality(counters);
	s2 = estimateCardinality(set2counters);
	updateCounters(counters, set2counters, ucounters);
	union_size = estimateCardinality(ucounters);

	// Unique blocks in set2
	//return (union_size - s1);

	// Matching blocks (intersection)
	return (s1 + s2 - union_size);	

	//if ((union_size <= (ublocks + set2size)) && (union_size >= ublocks))
		//return (union_size - ublocks);
	//else return set2size;
}

/*
void StorageNode::insertRequest(FileBlocks *fb, int *bptr, int op) {
	pthread_mutex_lock(&lck);
	if (reqcount == QLEN) { 
		// Request queue full, so wait
		pthread_cond_wait(&cond);
	}

	// Add entry
	fblks[tail] = fb;
	busyptr[tail] = bptr;
	operation[tail] = op;

	reqcount++;
	tail = (tail + 1) % QLEN;

	// Wakeup the storage node thread
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&lck);
}

void StorageNode::insertRequest(segment4k *s, int *bptr, int op) {
	pthread_mutex_lock(&lck);
	if (reqcount == QLEN) { 
		// Request queue full, so wait
		pthread_cond_wait(&cond);
	}

	// Add entry
	segs[tail] = s;
	busyptr[tail] = bptr;
	operation[tail] = op;

	reqcount++;
	tail = (tail + 1) % QLEN;

	// Wakeup the storage node thread
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&lck);
}

void StorageNode::deleteRequest(void) {
	pthread_mutex_lock(&lck);
	segs[head] = NULL;
	*busyptr[head] = 0; // FB/SEG is free to use/not busy
	busyptr[head] = NULL;
	reqcount--;
	head = (head+1) % QLEN;

	// Wakeup the segment/file blocks sending thread, if
	// waiting on condition object
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&lck);
}

int StorageNode::checkAndWait(void) {
	pthread_mutex_lock(&lck);
	if (reqcount == 0) 
		pthread_cond_wait(&cond, &lck);
	pthread_mutex_unlock(&lck);
}

void StorageNode::setCompleted(void) {
	pthread_mutex_lock(&lck);
	completed = 1;
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&lck);
}

void StorageNode::processNextRequest(void) {
	if (operation[head] == OP_READ) {
		switch(dtype) {
		case	DEDUPNATIVE:
			processSegmentReadNative(segs[head], this);
			break;
		case	DEDUPHDS:
			processSegmentReadHDS(segs[head], this);
			break;
		case	DEDUPFULL:
			processSegmentReadFull(segs[head], this);
			break;
		case	DEDUPAPP:
			processFileReadApp(fblks[head], this);
			break;
		}
	}
	else {
		switch(dtype) {
		case	DEDUPNATIVE:
			processSegmentWriteNative(segs[head], this);
			break;
		case	DEDUPHDS:
			processSegmentWriteHDS(segs[head], this);
			break;
		case	DEDUPFULL:
			processSegmentWriteFull(segs[head], this);
			break;
		case	DEDUPAPP:
			processFileWriteApp(fblks[head], this);
			break;
		}
	}
}

void *storageNodeThread(void *node) {
	StorageNode *sn = (StorageNode *)node;

	while (1) {
		sn->checkAndWait();
		// Check if processing completed?
		if (sn->completed == 1) break;

		// Otherwise there must be a request pending
		sn->processNextRequest();

		// Mark not busy, and delete the request from queue
		sn->deleteRequest();
	}
}
*/

// Hybrid Deduplication main function

#include <iostream>
#include <fstream>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "dedupconfig.h"
#include "dedupn.h"
//#include "cache.h"
#include "cachemem.h"
//#include "bucket.h"
//#include "hbucket.h"
#include "bitmap.h"
//#include "darc.h"
//#include "lru.h"
#include "metadata.h"
#include "iotracereader.h"
#include "util.h"
#include "iotraces.h"
//#include "metadata.h"

void cacheMemStats(void);

//string dcstr = "datacache";
//#ifdef	DARCP
//#ifdef	HOME
//DARC dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE, 512);
//#else
//DARC dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
//#endif
//#else
//#ifdef	HOME
//LRU dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE);
//#else
//LRU dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE);
//#endif
//#endif

IOTraceReader *trace[10];
//MDMaps *md;

string dummyfile="/dev/null";
fstream dummy;

//unsigned long int prevblock[2];

unsigned int reccount100000 = 1;
unsigned long long int curTime;

/************************************************/
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT_LDC = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_HIT_INDIRECT = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS_LDC = 0;
//unsigned long long int DATA_BLOCK_READ_CACHE_MISS_INDIRECT = 0;
//unsigned long long int DATA_BLOCK_READ_COUNT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_HIT = 0;
//unsigned long long int DATA_BLOCK_WRITE_CACHE_MISS = 0;
//unsigned long long int DATA_BLOCK_WRITE_COUNT = 0;
//unsigned long long int DATA_BLOCK_EFFECTIVE_WRITES = 0;
//unsigned long long int DATA_BLOCK_FLUSH_WRITES = 0;
//unsigned long long int UNIQUE_BLOCK_COUNT = 0;

//unsigned long long int DISK_READ_COUNT[2] = {0};
//unsigned long long int DISK_READ_BCOUNT[2] = {0};
//unsigned long long int DISK_WRITE_COUNT[2] = {0};
//unsigned long long int DISK_WRITE_BCOUNT[2] = {0};
//double DISK_READ_TOT_TIME[2] = {0};             // Time in mille seconds
//double DISK_WRITE_TOT_TIME[2] = {0};
//double DISK_READ_MIN_TIME[2] = {10000}; // Time in mille seconds
//double DISK_READ_MAX_TIME[2] = {0};
//double DISK_WRITE_MIN_TIME[2] = {10000};        // Time in mille seconds
//double DISK_WRITE_MAX_TIME[2] = {0};
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

double SEGCOUNT3 = 0.0;
double SEGLENGTH3 = 0.0;

int OPERATION;

/************************************************/
double r_var[1028], r_mean[1028], r_min[1028], r_max[1028];
unsigned long int r_n[1028] = {0}, w_n[1028] = {0};
double w_var[1028], w_mean[1028], w_min[1028], w_max[1028];
double w_flush=0;

//double t1_disk0 = 0, t1_disk1 = 0, t2_disk0 = 0, t2_disk1 = 0;
//double t3_disk0 = 0, t3_disk1 = 0;
//double t1_clk = 0, t2_clk = 0, t3_clk = 0, t0_hash = 0;
/*
 * sn^2 = (n-2)/(n-1)*sn-1^2 + 1/n*(xn - mean(xn-1)^2
 * mean(xn) = ((n-1)*mean(xn-1) + xn) / n;
 */


extern unsigned int     cemcount;
extern unsigned int     fcemcount;
extern unsigned int     b4kmcount;
extern unsigned int     prmcount;
extern unsigned int     imcount;

double t1, t2, t3, t4;

#ifdef	WEB
#define	MAX_SEG_SIZE_USED	(MAX_SEG_SIZE / 2)
#else
#define	MAX_SEG_SIZE_USED	MAX_SEG_SIZE
#endif

void update_statistics(double var[], double mean[],
		double minimum[], double maximum[],
		unsigned long int  N[], int seglen, unsigned int microsec) {
	unsigned long int n;

	n = ++N[seglen-1];
	if (n > 1) {
		var[seglen-1] = ((n-2)*var[seglen-1]/(n-1)) +
			((microsec - mean[seglen-1])*
			(microsec - mean[seglen-1])/n);
		mean[seglen-1] = ((n-1)*mean[seglen-1] + microsec) / n;

		if (minimum[seglen-1] > microsec) minimum[seglen-1] = microsec;
		if (maximum[seglen-1] < microsec) maximum[seglen-1] = microsec;
	}
	else {
		var[seglen-1] = 0.0;
		mean[seglen-1] = microsec;
		minimum[seglen-1] = microsec;
		maximum[seglen-1] = microsec;
	}
}

void countSegmentLength(unsigned int pba[], unsigned int seg_size) {
	unsigned int i;
	int diff;

	SEGLENGTH3 += seg_size;

	// Already not existing
	i = 1;
	while (i<seg_size) {

		diff = pba[i] - pba[i-1];

		if (abs(diff) > 2) {
			SEGCOUNT3++;
		}
		i++;

	}

	SEGCOUNT3++;    // Last segment
}

//===========================================================================

void getOldMaps(struct segment *s, MDMaps *m, unsigned int pba[],
unsigned int bnos[], unsigned int fbnos[], unsigned int refs[]) {
	unsigned int i;

	for (i=0; i<s->seg_size; i++) {
		m->bt->searchValWrap(s->seg_start+i, &pba[i]);

		if (pba[i] != 0) {
			m->pb->searchValWrap(pba[i], &bnos[i]);

			//if (bnos[i] > HASH_BUCK_COUNT) {
				//refs[i] = bkt->getRefCountWrap(bnos[i], pba[i]);
				//fbnos[i] = bkt->getFirstBucketWrap(bnos[i]);
			//}
			//else {
				//refs[i] = hbkt->getRefCountWrap(bnos[i], pba[i]);
				//fbnos[i] = bnos[i];
			//}
		}
	}
}


void dedupSmallSegment(struct segment *s, MDMaps *m, unsigned int pba[], 
		unsigned int bnos[], unsigned int opba[], unsigned int obno[], 
		unsigned int oref[]) {
	unsigned int pba1;
	unsigned int refcount;
	unsigned int hbno;
	int retval;
	CacheEntry *c1;
	
	// Find the hash bucket corresponding to the hash
	// Search for the exact matching hash
	m->hbkt->searchValWrap(s->seg_hash[0], &pba1, &refcount);
			
#ifdef	__DEBUG__
	cout << "small seg bnos[0] = " << obno[0] << " pba[0] = " << opba[0] << " pba1 " << pba1 << endl;
#endif
	// If necessary insert the hash
	// Appropriately increment/decrement pba reference count
	// insert/update/delete the entries in old buckets and 
	// lba-pba-bno btree
	hbno = (s->seg_hash[0][0] | (s->seg_hash[0][1] << 8)) + 1;
	if (opba[0] == 0) {
		if (pba1 == 0) { 
			// This fp is not found in the hash
			// Add new entry
			pba[0] = allocBlocks(1);
			assert(pba[0] > 0);
			UNIQUE_BLOCK_COUNT ++;
			m->hbkt->insertValWrap(hbno, pba[0], s->seg_hash[0], 1);
			//dupcounts[0][0]++;
			m->bt->insertValWrap(s->seg_start, pba[0]);
			m->pb->insertValWrap(pba[0], hbno);
		}
		else {
			pba[0] = pba1;
			//dupcounts[0][1]++;
			// Increment the pba reference count and add lba entry
			m->hbkt->incrementRefValWrap(hbno, pba1);
			m->bt->insertValWrap(s->seg_start, pba1);
		}
	}
	else {
		// If pba1 and pba[0] are same
		// nothing needs to be done.
		if (pba1 != opba[0]) {
			if (obno[0] > HASH_BUCK_COUNT) 
				retval = m->bkt->decrementRefValWrap(obno[0], opba[0]); 
			else
				retval = m->hbkt->decrementRefValWrap(obno[0], opba[0]);
			if (retval == 0) {
				m->pb->deleteValWrap(opba[0]);
				// Free the block, and make
				// the cache entry reusable
				freeBlock(opba[0]);
				c1 = dc.searchCache(opba[0]);
				if (c1 != NULL) {
					dc.deleteEntry(c1);
					cefree(c1);
				}
				UNIQUE_BLOCK_COUNT--;
			}

			if (pba1 == 0) {
				pba1 = allocBlocks(1);
				assert(pba1 > 0);
				UNIQUE_BLOCK_COUNT ++;
				m->hbkt->insertValWrap(hbno, pba1, s->seg_hash[0], 1);
				m->pb->insertValWrap(pba1, hbno);
				//dupcounts[0][0]++;
			}
			else {
				m->hbkt->incrementRefValWrap(hbno, pba1);
				//dupcounts[0][1]++;
			}

			// Update lba2pba
			m->bt->updateValWrap(s->seg_start, pba1);
			pba[0] = pba1;
		}
	}
	bnos[0] = hbno;
}

void dedupLargeSegment(struct segment *s, MDMaps *m, unsigned int pba[], 
		unsigned int bnos[], unsigned int opba[], unsigned int obno[], 
		unsigned int fbno[], unsigned int oref[]) {
	unsigned int i; 
	unsigned int bn, bnonew, seg_pba; 
	unsigned int bbnos[MAX_SEG_SIZE_USED], dbno[MAX_SEG_SIZE_USED];
	unsigned int bpba[MAX_SEG_SIZE_USED], npba[MAX_SEG_SIZE_USED], dpba[MAX_SEG_SIZE_USED]; 
	//unsigned int reused[MAX_SEG_SIZE_USED];
	unsigned int brefcounts[MAX_SEG_SIZE_USED];
	unsigned int dpbacount;

	unsigned char *rid; 
	CacheEntry *c1;
	unsigned int retval;

	// Identify segments from the LBA ordered sequences, as much long as 

	// For larger segments:

	// Identify minhash for the complete segment 
	// Find the bucket corresponding to the minhash(rid)
	// Find minhash and identify the matching bucket

	dpbacount = 0;
	rid = minHash(s->seg_hash, s->seg_size);
	bn = m->bkt->findBucket(rid);

#ifdef	__DEBUG__
	cout << "large segment bkt " << bn << endl;
#endif
	if (bn == 0) {	
		// New bucket is to be created
		// Add this segment to new bucket
		bnonew = m->bkt->addNewBucket(rid);
		assert(bnonew > 0);

		// Allocate new blocks 
		seg_pba = allocBlocks(s->seg_size);
		assert(seg_pba > 0);
		UNIQUE_BLOCK_COUNT += s->seg_size;

		// New bucket, so free all lba-pba-bno entries
		// Decrement pba reference counts 
		// for old pba values
		for (i=0; i<s->seg_size; i++) {
			if (opba[i] != 0) {
				m->bt->deleteValWrap(s->seg_start+i);

				if (obno[i] <= HASH_BUCK_COUNT)
					retval = m->hbkt->decrementRefValWrap(obno[i], opba[i]);
				else
					retval = m->bkt->decrementRefValWrap(obno[i], opba[i]);
				if (retval == 0) {
					m->pb->deleteValWrap(opba[i]);
					// Free the block, and make
					// the cache entry reusable
					freeBlock(opba[i]);
					c1 = dc.searchCache(opba[i]);
					if (c1 != NULL) {
						dc.deleteEntry(c1);
						cefree(c1);
					}
					UNIQUE_BLOCK_COUNT--;
				}
			}
		}

		// Add lba-pba-bno new entries
		for (i=0; i<s->seg_size; i++) {
			npba[i] = seg_pba + i;
			bnos[i] = bnonew + HASH_BUCK_COUNT;
			m->bkt->insertValWrap(bnos[i], npba[i], s->seg_hash[i], 1);
			m->bt->insertValWrap(s->seg_start+i, npba[i]);
			m->pb->insertValWrap(npba[i], bnos[i]);
			pba[i] = npba[i];
		}
		//dupcounts[s->seg_size-1][0]++;
	}
	else {
		//dupcnt = s->seg_size;

		for (i=0; i<s->seg_size; i++) 
			bpba[i] = bbnos[i] = npba[i] = 0;

		// First search for all hashes and get the 
		// corresponding, bnos, pba's and lba's. 
		// DO NOT USE LBA's, because multiple lba's 
		// might be mapped to same pba and in such a 
		// case any one lba is returned, may be 
		// depending on the order.

		// Search for hashes in the cache first, which 
		// might not have been written to buckets yet.

		m->bkt->searchHashesWrap(bn+HASH_BUCK_COUNT, s, bpba, bbnos, brefcounts);
		// First add missing hashes 
		for (i=0; i<s->seg_size; i++) {
			if (bpba[i] == 0) {
				// Hash not found in the bucket
				npba[i] = allocBlocks(1);
				assert(npba[i] > 0);
				UNIQUE_BLOCK_COUNT ++;
				//dupcnt--;
			}
		}
		//dupcounts[s->seg_size-1][dupcnt]++;

		for (i=0; i<s->seg_size; i++) {
			if (bpba[i] != 0) {
				// Hash found in the bucket
				if (opba[i] != bpba[i]) {
					if (opba[i] != 0) {
						dpba[dpbacount] = opba[i];
						dbno[dpbacount++] = obno[i];
					}
					m->bkt->incrementRefValWrap(bbnos[i], bpba[i]);

					if (opba[i] == 0)
						m->bt->insertValWrap(s->seg_start+i, bpba[i]);
					else
						m->bt->updateValWrap(s->seg_start+i, bpba[i]);
				}
				pba[i] = bpba[i];
			}
			else {
				// Hash not found in the bucket
				if (opba[i] != 0) {
					// Lba in another bucket/hash bucket
					// Remove the lba entry from old bucket
						dpba[dpbacount] = opba[i];
						dbno[dpbacount++] = obno[i];
						// Existing one will be updated, no need of deleting the old one.
				}
				m->bkt->insertValWrap(bn+HASH_BUCK_COUNT, npba[i], s->seg_hash[i], 1);
				if (opba[i] == 0)
					m->bt->insertValWrap(s->seg_start+i, npba[i]);
				else
					m->bt->updateValWrap(s->seg_start+i, npba[i]);
				m->pb->insertValWrap(npba[i], bn+HASH_BUCK_COUNT);
				pba[i] = npba[i];
			}
		}

		// Decrement the reference counts of the 
		// old remembered pba's
		for (i=0; i<dpbacount; i++) {
			if (dbno[i] <= HASH_BUCK_COUNT)
				retval = m->hbkt->decrementRefValWrap(dbno[i], dpba[i]);
			else
				retval = m->bkt->decrementRefValWrap(dbno[i], dpba[i]);
			if (retval == 0) {
				m->pb->deleteValWrap(dpba[i]);
				// Free the block, and make
				// the cache entry reusable
				freeBlock(dpba[i]);
				c1 = dc.searchCache(dpba[i]);
				if (c1 != NULL) {
					dc.deleteEntry(c1);
					cefree(c1);
				}
				UNIQUE_BLOCK_COUNT--;
			}
		}
	}
	//cout << "dedupLargeSegment step 2" << endl;
	countSegmentLength(pba, s->seg_size);
	//cout << "dedupLargeSegment step 3" << endl;
}


void dedupSegment(struct segment *s, MDMaps *m, unsigned pba[]) {
	unsigned int i; 
	unsigned int bno[MAX_SEG_SIZE_USED], fbno[MAX_SEG_SIZE_USED];
	unsigned int opba[MAX_SEG_SIZE_USED], obno[MAX_SEG_SIZE_USED];
	unsigned int oref[MAX_SEG_SIZE_USED];

	// Identify segments from the LBA ordered sequences, as much long as 
	// possible. 
	// For larger segments:
	// Identify minhash for the complete segment 
	// Find the bucket corresponding to the minhash(rid)
	// Then search for hashes in the bucket
	// Get the entries from lba-pba-bno btree if exists
	// Then as per the available hashes and exisitng lba-pba-bno values
	// insert/update the entries in the bucket and 
	// update/insert the lba-pba-bno btree entries
	// Increment/decrement pba reference counts.
	//
	// For smaller segments (block 1):
	// Find the hash bucket corresponding to the hash
	// Search for the exact matching hash
	// If necessary insert the hash
	// Appropriately increment/decrement pba reference count
	// insert/update/delete the entries in old buckets and 
	// lba-pba-bno btree
	//
	// Move the lba blocks from ldc to dc, with appropriately
	// mapped pba values.

	
	// Add md5 hash calculation times
#ifdef	HOME
	clock_add(s->seg_size*4);
	//t0_hash += (s->seg_size*4);
#else
	clock_add(s->seg_size*30);
	//t0_hash += (s->seg_size*30);
#endif

	for (i=0; i<s->seg_size; i++)
		opba[i] = pba[i] = obno[i] = fbno[i] = bno[i] = oref[i] = 0;

	getOldMaps(s, m, opba, obno, fbno, oref);	

	//cout <<	"dedupSegment 1" << endl;
	// For smaller segments (block 1):
	if (s->seg_size == 1) 
		dedupSmallSegment(s, m, pba, bno, opba, obno, oref);
	else {
		dedupLargeSegment(s, m, pba, bno, opba, obno, fbno, oref);
	}
}

// Read request processing. Indeintify the corresponding pba's 
void processReadRequest(struct segment *s, MDMaps *m, unsigned int pba[]) {
	int i; 
	//unsigned long int odtime, octime;
	//int flag = 0;


	for (i=0; i<s->seg_size; i++) pba[i] = 0; 
	
	// First search in the present write buffers with the corresponding
	// LBA values. Assuming that the corresponding data is available in 
	// the ldc cache, process those LBA reads directly from cache. The 
	// corresponding Unmapped pba entries are retained as zero's
	
	// If pba's are found for the corresponding lba's use the pba's and 
	// search dc cache for the data blocks.

	// If there is no old LBA or corresponding pba, then this is read
	// corresponding to old write, which is not in the trace. So generate 
	// a corresponding write operation with old time stamp value.

/*
	flag = 0;
	for (i=0; i<s->seg_size; i++)  {
		m->bt.searchValWrap(s->seg_start+i, &pba[i]);
		if (pba[i] == 0) {
			OPERATION = OPERATION_WRITE;
			odtime = disk_suspend();
			octime = clock_suspend();
			dedupSegment(s, pba);
			OPERATION = OPERATION_READ;
			disk_start();
			clock_start();
			disk_timeadd(odtime);
			clock_add(octime);
			flag = 1;
			break;
		}
	}

	if (flag == 1) // Is segment deduplicated now?
*/
		for (i=0; i<s->seg_size; i++) // Find new mappings
			m->bt->searchValWrap(s->seg_start+i, &pba[i]);

	// For the missing blocks search in write request buffer, 
	// if necessary add write requests at the beginning
}

void processWriteRequest(struct segment *s, MDMaps *m, unsigned int pba[]) {

	dedupSegment(s, m, pba);

	// First search in the present write buffers with the corresponding
	// LBA values. Overwrite the old request. 
	// The corresponding Unmapped pba entries are retained 
	// as zero's. For such writes add the data to the ldc cache.
}

void readSegmentDC(unsigned int pba, unsigned int count, unsigned long long int ts, unsigned long long int incr) {
	unsigned int j, k;
	unsigned int pba1, pba2;
	unsigned int rtime = ts;
	unsigned int scount;
	CacheEntry *c;
	unsigned short cflags;
	int wcount;

#ifdef	__DEBUG__
	cout << "readSegmentDC pba " << pba << " count " << count << endl;
#endif
	// Search for the whole segment?
	for (j=0; j<count; j++) {
		pba1 = pba + j;
		c = dc.searchCache(pba1);
		if (c != NULL) {
#ifndef	HOME
			DATA_BLOCK_READ_CACHE_HIT += 8;
#else
			DATA_BLOCK_READ_CACHE_HIT++;
#endif
			c->ce_refcount++;
			c->ce_rtime = rtime;
			dc.c_rrefs++;
			rtime += incr;
			curTime += incr;
#ifdef	DARCP
			dc.repositionEntry(c);
#endif
		}
		else {
			// Find the sequence of blocks not in 
			// data cache, and issue single request
			scount = 1;
			pba2 = pba1+1;
			j++;

			while ((j < count) && ((c = dc.searchCache(pba2)) == NULL)) {
				j++; scount++; pba2++;
			}
			j--; //Go back to the last block
		
#ifndef	HOME
			DiskRead(curTime, DATADISK, 8*pba1, 8*scount);
			DATA_BLOCK_READ_CACHE_MISS += (8 * scount);
#else
			DiskRead(curTime, DATADISK, pba1, scount);
			DATA_BLOCK_READ_CACHE_MISS += scount;
#endif
			cflags = CACHE_ENTRY_CLEAN;
			curTime += (incr * scount);

			// Add missing data block entries to the cache
			for (k=0; k<scount; k++) {
				pba2 = pba1 + k;
#ifndef	HOME
				wcount = dc.addReplaceEntry(dummy, pba2, NULL, 4096, rtime, 0, DATADISK, cflags, NULL, &c);
#else
				wcount = dc.addReplaceEntry(dummy, pba2, NULL, 512, rtime, 0, DATADISK, cflags, NULL, &c);
#endif
				DATA_BLOCK_EFFECTIVE_WRITES += wcount;
			}

			rtime += (incr * scount);
		}
	}
}

void writeSegmentDC(unsigned int pba, unsigned int count, unsigned long long int ts, unsigned long long int incr) {
	unsigned int j;
	unsigned int pba1;
	unsigned int wtime = ts;
	CacheEntry *c;
	unsigned short cflags;
	int wcount;

	// Search for the whole segment?
	for (j=0; j<count; j++) {
		pba1 = pba + j;
		c = dc.searchCache(pba1);
		if (c != NULL) {
#ifndef HOME
			DATA_BLOCK_WRITE_CACHE_HIT += 8;
#else
			DATA_BLOCK_WRITE_CACHE_HIT++;
#endif
			c->ce_refcount++;
			c->ce_wrefcount++;
			dc.c_wrefs++;
			c->ce_wtime = wtime;
			if ((c->ce_flags & CACHE_ENTRY_DIRTY) == 0) {
				c->ce_flags |= CACHE_ENTRY_DIRTY;
				dc.c_dcount++;
			}
#ifdef  DARCP
			dc.repositionEntry(c);
#endif
		}
		else {
			cflags = CACHE_ENTRY_DIRTY;
#ifndef HOME
			DATA_BLOCK_WRITE_CACHE_MISS += 8;
#else
			DATA_BLOCK_WRITE_CACHE_MISS++;
#endif

#ifndef HOME
			wcount = dc.addReplaceEntry(dummy, pba1, NULL, 4096, 0, wtime, DATADISK, cflags, NULL, &c);
#else
			wcount = dc.addReplaceEntry(dummy, pba1, NULL, 512, 0, wtime, DATADISK, cflags, NULL, &c);
#endif
			DATA_BLOCK_EFFECTIVE_WRITES += wcount;
		}
		wtime += incr;
		curTime += incr;
	}
}


void readSegmentData(struct segment *s, unsigned int pba[], unsigned long long int ts, unsigned long long int incr) {
	unsigned long long int rtime;
	unsigned int i;
	unsigned int pbastart, count;

	// Search for the data blocks segment by segment in the cache
	// with the representative pba number
	i = 0;
	rtime = ts;

	while (i < s->seg_size) {
		pbastart = pba[i];
		count = 1;
		i++;

		while ((i < s->seg_size) && (pba[i] == (pba[i-1] + 1))) {
			count++;
			i++;
		}

		readSegmentDC(pbastart, count, rtime, incr);
		rtime += (incr * count);
	}
}

void writeSegmentData(struct segment *s, unsigned int pba[], unsigned long long int ts, unsigned long long int incr) {
	unsigned long long int wtime;
	unsigned int i;
	unsigned int pbastart, count;

	// Search for the data blocks segment by segment in the cache
	// with the representative pba number
	i = 0;
	wtime = ts;

	while (i < s->seg_size) {
		pbastart = pba[i];
		count = 1;
		i++;

		while ((i < s->seg_size) && (pba[i] == (pba[i-1] + 1))) {
			count++;
			i++;
		}

		writeSegmentDC(pbastart, count, wtime, incr);
		wtime += (incr * count);
	}
}
void processReadTrace(int tno, MDMaps *m) {
	int next_pid = -1;
	int i, j; 
	int op; 
	unsigned int pba[MAX_SEG_SIZE];
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct segment s;
	struct segment4k s1;
	struct segmenthome s2;
	//unsigned int twritecount;
	int l;
	int flag, len;
	int retval; //, trace15 = 0;
	unsigned int ind;
	unsigned long dtime, overhead;
	int seg_size;
	IOTraceReader *trace;

#ifdef	DARCP
	unsigned long int DARCINTREFCOUNT;
	unsigned int darcInterval = 0;

	DARCINTREFCOUNT = 512 * (DATA_CACHE_SIZE / 512);
	unsigned long long int lastDecayTime = 0;
#endif

	reccount100000 = 1;

	if (tno == 2)
		trace = new IOTraceReaderHome();
	else    trace = new IOTraceReader4K();

	if ((trace->openTrace(frnames[tno], fbsize[tno]) != 0)) {
		perror("IO Trace opening failure:");
		exit(0);
	}

	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	while (1) {
		if (tno != 2)
			retval = trace->readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		else
			retval = trace->readNextSegment(&next_pid, &op, &ts1, &ts2, &s2);

		if (retval == 0) break;

		READ_REQ_COUNT++;
		OPERATION = OPERATION_READ;
#ifndef	HOME
		DATA_BLOCK_READ_COUNT += (s1.seg_size * 8);
#else
		DATA_BLOCK_READ_COUNT += s2.seg_size;
#endif

#ifndef	HOME
		s1.seg_start /= 8;
		if (s1.seg_size > 1)
			incr = (ts2 - ts1) / (s1.seg_size - 1);
		else	incr = 2;
		seg_size = s1.seg_size;
#else
		if (s2.seg_size > 1)
			incr = (ts2 - ts1) / (s2.seg_size - 1);
		else	incr = 2;
		seg_size = s2.seg_size;
#endif
		curTime = ts1;
		if (incr <= 0) incr = 2;

		clock_start();
		disk_start();

		l = 0;
		// Split and process home requests which are very larger
		while (l < seg_size) {
#ifndef	HOME
			s.seg_start = s1.seg_start + l;
			memcpy(s.seg_hash[0], s1.seg_hash[l], 16);
			len = 1;
			for (i=1; (i<MAX_SEG_SIZE_USED) && ((l + i) < s1.seg_size); i++) {
				flag = 1;
				for (j=0; j < i; j++) {
					if (memcmp(s.seg_hash[j], s1.seg_hash[l+i], 16) == 0) {
						flag = 0;
						break;
					}
				}
				if (flag == 0) break;
				
				memcpy(s.seg_hash[i], s1.seg_hash[l+i], 16);
				len++;
			}
			s.seg_size = len;
#else
			s.seg_start = s2.seg_start + l;
			memcpy(s.seg_hash[0], s2.seg_hash[l], 16);
			len = 1;
			for (i=1; (i<MAX_SEG_SIZE_USED) && ((l + i) < s2.seg_size); i++) {
				flag = 1;
				for (j=0; j < i; j++) {
					if (memcmp(s.seg_hash[j], s2.seg_hash[l+i], 16) == 0) {
						flag = 0;
						break;
					}
				}
				if (flag == 0) break;
				
				memcpy(s.seg_hash[i], s2.seg_hash[l+i], 16);
				len++;
			}
			s.seg_size = len;
#endif

#ifdef	__DEBUG	
			cout <<  "\n\t\tseg : " << s.seg_start << " , " << 8*s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

			processReadRequest(&s, m, pba);
		
			// Search for the data blocks segment by segment in the cache
			// with the representative pba number
			if (op == OP_READ) readSegmentData(&s, pba, ts1, incr);

			ts1 += (incr * s.seg_size);
			l += s.seg_size;
		}

		overhead = clock_stop();
		dtime = disk_stop();

                ind = dtime * 1000 + overhead;
#ifdef	HOME
		update_statistics(r_var, r_mean, r_min, r_max, r_n, seg_size, ind);
#else
		update_statistics(r_var, r_mean, r_min, r_max, r_n, seg_size*8, ind);
#endif

		if (trace->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

		//if ((lastGCTime == 0) && (lastFlushTime == 0)) { 
			// Not yet initialized
			//lastFlushTime = lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			//clock_start();
			//disk_start();

			//bt.flushCache(METADATADISK);
			//pb.flushCache(METADATADISK);
			// Dummy writing ...
			//bkt->flushCache();
			//hbkt->flushCache();
			//twritecount = dc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			//twritecount = ldc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			//updateBitMap();
			lastFlushTime = (ts2 / 1000000);

			//overhead = clock_stop();
			//dtime = disk_stop();

			//ind = dtime * 1000 + overhead; // Microseconds
			//w_flush += ind;
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			//bt.flushCache(METADATADISK);
			//pb.flushCache(METADATADISK);
			// Dummy writing ...
			//bkt->flushCache();
			//hbkt->flushCache();
			//twritecount = dc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			//twritecount = ldc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			//updateBitMap();
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef  DARCP
                if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
                        //dc.decayWeights();
                        //ldc.decayWeights();
                        lastDecayTime = ts2 / 1000000;
                }


                // New interval starting, once per every 10 minutes
		if (((darcInterval + 1) * DARCINTREFCOUNT) > 
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
                        dc.newInterval();
                        //ldc.newInterval();
			darcInterval++;
                        //lastDARCTime = ts2 / 1000000;
                }
#endif

		// Garbage collection once in an hour
		//if (((ts2 / 1000000) - lastGCTime) > (60000 * 60)) {
			//gc();
			//lastGCTime = ts2 / 1000000;
		//}
	}
	cout <<"\n\nTotal read records processed \t\t\t: " << trace->getCurCount() << endl;
}

int main(int argc, char** argv) {
	int next_pid = -1;
	int i, j; 
	int op; 
	unsigned int pba[MAX_SEG_SIZE];
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct segment s;
	struct segment4k s1;
	struct segmenthome s2;
	unsigned int twritecount;
	int l;
	int flag, len;
	int retval; //, trace15 = 0;
	unsigned int ind;
	unsigned long dtime, overhead;
	int tno;
	int seg_size;
	struct timespec tval;

#ifdef	DARCP
	unsigned long int DARCINTREFCOUNT;
	unsigned int darcInterval = 0;

	DARCINTREFCOUNT = 512 * (DATA_CACHE_SIZE / 512);
	unsigned long long int lastDecayTime = 0;
#endif

#ifdef	MAIL
	tno = 0;
#endif
#ifdef	WEB
	tno = 1;
#endif
#ifdef	HOME
	tno = 2;
#endif
#ifdef  LINUX
        tno = 3;
#endif
#ifdef  BOOKPPT
        tno = 4;
#endif
#ifdef  VIDIMG
        tno = 5;
#endif


	initCacheMem();

	md = new MDMaps("blockidx.dat", "pbaidx.dat", "hashidx.dat",
			"filehashidx.dat", "buckets.dat", "hashbuckets.dat",
			"pbaref.dat", "fblist.dat", DEDUPHDS);

	dummy.open(dummyfile.c_str(), ios::in | ios::out | ios::binary);
	openBitMap();

/*
	trace[0] = new IOTraceReader4K();
	trace[1] = new IOTraceReader4K();
	trace[2] = new IOTraceReaderHome();

	//if (trace.openTrace(fnames[tno], 14) != 0) {
	//if (trace.openTrace(fnames[tno], 1) != 0) {
	if ((trace[0]->openTrace(fnames[0], 21, 4096) != 0) ||
		(trace[1]->openTrace(fnames[1], 21, 4096) != 0) ||
		(trace[2]->openTrace(fnames[2], 21, 512) != 0)) {
		perror("IO Trace opening failure:");
		exit(0);
	}
*/
	//if (trace.openTrace(fnames[tno], 14) != 0) {
	//if (trace.openTrace(fnames[tno], 1) != 0) {
	//if (trace.openTrace(fnames[tno], 21) != 0) {
		//perror("IO Trace opening failure:");
		////exit(0);
	//}

//	if (trace.openTrace(fnames[tno], 1) != 0) {
//		perror("IO Trace opening failure:");
//		exit(0);
//	}

	for (i=0; i<6; i++) {
		if (i == 2)
			trace[i] = new IOTraceReaderHome();
		else    trace[i] = new IOTraceReader4K();

		if ((trace[i]->openTrace(fnames[i], fcount[i],
			fbsize[i]) != 0)) {
			perror("IO Trace opening failure:");
			exit(0);
		}
	}

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t1_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);

	// In each trace all requests are to the same device. 
	// device numbers are not considered.
	while (1) {
		if (tno != 2)
			retval = trace[tno]->readNextSegment(&next_pid, &op, &ts1, &ts2, &s1);
		else
			retval = trace[tno]->readNextSegment(&next_pid, &op, &ts1, &ts2, &s2);

		if (retval == 0) break;

		if (op == OP_READ) {
			READ_REQ_COUNT++;
			OPERATION = OPERATION_READ;
#ifndef	HOME
			DATA_BLOCK_READ_COUNT += (s1.seg_size * 8);
#else
			DATA_BLOCK_READ_COUNT += s2.seg_size;
#endif
		}
		else {
			WRITE_REQ_COUNT++;
			OPERATION = OPERATION_WRITE;
#ifndef	HOME
			DATA_BLOCK_WRITE_COUNT += (s1.seg_size * 8);
#else
			DATA_BLOCK_WRITE_COUNT += s2.seg_size;
#endif
		}
#ifndef	HOME
		s1.seg_start /= 8;
		if (s1.seg_size > 1)
			incr = (ts2 - ts1) / (s1.seg_size - 1);
		else	incr = 2;
		seg_size = s1.seg_size;
		t0_hash += (seg_size * 30);
#else
		if (s2.seg_size > 1)
			incr = (ts2 - ts1) / (s2.seg_size - 1);
		else	incr = 2;
		seg_size = s2.seg_size;
		t0_hash += (seg_size * 4);
#endif
		curTime = ts1;
		if (incr <= 0) incr = 2;

		clock_start();
		disk_start();

		l = 0;
		// Split and process home requests which are very larger
		while (l < seg_size) {
#ifndef	HOME
			s.seg_start = s1.seg_start + l;
			memcpy(s.seg_hash[0], s1.seg_hash[l], 16);
			len = 1;
			for (i=1; (i<MAX_SEG_SIZE_USED) && ((l + i) < s1.seg_size); i++) {
				flag = 1;
				for (j=0; j < i; j++) {
					if (memcmp(s.seg_hash[j], s1.seg_hash[l+i], 16) == 0) {
						flag = 0;
						break;
					}
				}
				if (flag == 0) break;
				
				memcpy(s.seg_hash[i], s1.seg_hash[l+i], 16);
				len++;
			}
			s.seg_size = len;
#else
			s.seg_start = s2.seg_start + l;
			memcpy(s.seg_hash[0], s2.seg_hash[l], 16);
			len = 1;
			for (i=1; (i<MAX_SEG_SIZE_USED) && ((l + i) < s2.seg_size); i++) {
				flag = 1;
				for (j=0; j < i; j++) {
					if (memcmp(s.seg_hash[j], s2.seg_hash[l+i], 16) == 0) {
						flag = 0;
						break;
					}
				}
				if (flag == 0) break;
				
				memcpy(s.seg_hash[i], s2.seg_hash[l+i], 16);
				len++;
			}
			s.seg_size = len;
#endif

#ifdef	__DEBUG	
			cout <<  "\n\t\tseg : " << s.seg_start << " , " << 8*s.seg_start << ", " << s.seg_size << ", " << seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

			if (op == OP_READ) processReadRequest(&s, md, pba);
			else processWriteRequest(&s, md, pba);
	
			//cout << "step 1" << endl;	
			// Search for the data blocks segment by segment in the cache
			// with the representative pba number
			if (op == OP_READ) readSegmentData(&s, pba, ts1, incr);
			else writeSegmentData(&s, pba, ts1, incr);
			//cout << "step 2" << endl;	

			ts1 += (incr * s.seg_size);
			l += s.seg_size;
		}

		overhead = clock_stop();
		dtime = disk_stop();

                ind = dtime * 1000 + overhead;
#ifdef	HOME
		update_statistics(w_var, w_mean, w_min, w_max, w_n, seg_size, ind);
#else
		update_statistics(w_var, w_mean, w_min, w_max, w_n, seg_size*8, ind);
#endif

		if (trace[tno]->getCurCount() >= (reccount100000 * 1000000)) {
			cout << trace[tno]->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

		//if ((lastGCTime == 0) && (lastFlushTime == 0)) { 
			// Not yet initialized
			//lastFlushTime = lastGCTime = ts2 / 1000000;// Milleseconds
		//}

		// For every 30 seconds flush the cache ?
#ifdef	FLUSH_TIME_30
		if (((ts2 / 1000000) - lastFlushTime) > 30000) {
			clock_start();
			disk_start();

			md->flushCache();
			//bt.flushCache(METADATADISK);
			//pb.flushCache(METADATADISK);
			// Dummy writing ...
			//bkt->flushCache();
			//hbkt->flushCache();
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			//twritecount = ldc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			updateBitMap();
			lastFlushTime = (ts2 / 1000000);

			overhead = clock_stop();
			dtime = disk_stop();

			ind = dtime * 1000 + overhead; // Microseconds
			w_flush += ind;
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			//bt.flushCache(METADATADISK);
			//pb.flushCache(METADATADISK);
			// Dummy writing ...
			//bkt->flushCache();
			//hbkt->flushCache();
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			//twritecount = ldc.flushCache(dummy, 0, DATADISK);
			//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			//DATA_BLOCK_FLUSH_WRITES += twritecount;
			updateBitMap();
			lastFlushTime = (ts2 / 1000000);
		}
#endif	

#ifdef  DARCP
                if (((ts2 / 1000000) - lastDecayTime) > (60000 * 60 * 6)) {
                        //dc.decayWeights();
                        //ldc.decayWeights();
                        lastDecayTime = ts2 / 1000000;
                }


                // New interval starting, once per every 10 minutes
		if (((darcInterval + 1) * DARCINTREFCOUNT) > 
			(DATA_BLOCK_READ_COUNT + DATA_BLOCK_WRITE_COUNT)) {
                        dc.newInterval();
                        //ldc.newInterval();
			darcInterval++;
                        //lastDARCTime = ts2 / 1000000;
                }
#endif

		// Garbage collection once in an hour
		//if (((ts2 / 1000000) - lastGCTime) > (60000 * 60)) {
			//gc();
			//lastGCTime = ts2 / 1000000;
		//}
	}

	clock_start();
	disk_start();

	//bt.flushCache(METADATADISK);
	//pb.flushCache(METADATADISK);
	//bkt->flushCache();
	//hbkt->flushCache();
	md->flushCache();
	updateBitMap();

	twritecount = dc.flushCache(dummy, 0, DATADISK);
	DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
	DATA_BLOCK_FLUSH_WRITES += twritecount;
	//twritecount = ldc.flushCache(dummy, 0, DATADISK);
	//DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
	//DATA_BLOCK_FLUSH_WRITES += twritecount;

	overhead = clock_stop();
	dtime = disk_stop();

	ind = dtime * 1000 + overhead; // Microseconds
	w_flush += ind;

	cout << "\n\nTotal write records processed \t\t\t: " << trace[tno]->getCurCount() << endl;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t2_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t2_disk0 = DISK_WRITE_TOT_TIME[0];
	t2_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	//==============================
	
	cout << "\n============ READ Trace ("
		<< frnames[tno] << ") processing =========\n\n";
	processReadTrace(tno, md);

	//===============================
	
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t3_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t3_disk0 = DISK_READ_TOT_TIME[0];
	t3_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	cout.setf(ios::fixed);
	cout.precision(4);

	cout << "\nt1_clk " << t1_clk << "\nt2_clk " << t2_clk << "\nt3_clk " << t3_clk << "\nt2_disk0 " << t2_disk0 << "\nt3_disk0 " << t3_disk0 << "\nt2_disk1 " << t2_disk1 << "\nt3_disk1 " << t3_disk1 << "\nt0_hash " << t0_hash << endl;

	cout << "Write throughput : " << (((DATA_BLOCK_WRITE_COUNT * 512.0 *
		10000000.0) / (t2_clk - t1_clk + t0_hash + ((t2_disk0 + t2_disk1)*1000)))
		/ (1024 * 1024)) << "MB / sec\n";
	cout << "Read throughput : " << (((DATA_BLOCK_READ_COUNT * 512.0 *
		10000000.0) / (t3_clk - t2_clk +
		((t3_disk0 + t3_disk1 - t2_disk1)*1000)))
		/ (1024 * 1024)) << "MB / sec\n";

	cout << "Working set size : " << WSS << "KB" << endl;
	cout << "Cache sizes used:\n" << "Data Cache size \t: " 
		<< (DATA_CACHE_SIZE / (1024)) 
		<< " KB\n";
       //	<< "Meta data cache size \t: " 
		//<< ((BUCKET_CACHE_SIZE + HBUCKET_CACHE_SIZE +
			//BUCKETINDEX_CACHE_SIZE + (2 * BLOCKINDEX_CACHE_SIZE3) + 
			//LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE + 
			//RID_CACHE_SIZE + WRB_CACHE_SIZE +
			//HLPL_CACHE_SIZE + LPL_CACHE_SIZE) / (1024)) << " KB\n";

	//cout << "\n\nFP Cache Hit count \t\t: " << FP_CACHE_HIT << endl
		//<< "FP Cache Miss count \t\t: " << FP_CACHE_MISS << endl
		//<< "FP CACHE COUNT   \t\t: " << fp_count << endl
		//<< "LBA to PBA Cache Hit Count \t: " << LBA2PBA_CACHE_HIT << endl
		//<< "LBA to PBA Cache Miss Count \t: " << LBA2PBA_CACHE_MISS << endl
		//<< " RID to bucket Cache hit \t\t:" << RID_CACHE_HIT << endl 
		//<< " RID to bucket Cache miss \t\t:" << RID_CACHE_MISS << endl << endl
		//<< " BNO to PBA Cache hit \t\t:" << BNO2PBA_CACHE_HIT << endl
		//<< " BNO to PBA Cache miss \t\t:" << BNO2PBA_CACHE_MISS << endl;

	cacheMemStats();

	for (i=0; i<2; i++) {
		cout << "\n\nDISK : " << i << endl;
		cout << "\nDISK_READ_COUNT \t: " << DISK_READ_COUNT[i] << endl
			<< "DISK_WRITE_COUNT \t: " << DISK_WRITE_COUNT[i] << endl
			<< "DISK_READ_BCOUNT \t: " << DISK_READ_BCOUNT[i] << endl
			<< "DISK_WRITE_BCOUNT \t: " << DISK_WRITE_BCOUNT[i] << endl
			<< "DISK_READ_TOT_TIME \t: " << DISK_READ_TOT_TIME[i] << endl
			<< "DISK_WRITE_TOT_TIME \t: " << DISK_WRITE_TOT_TIME[i] << endl;
	}

	cout << "\n\nDATA READ COUNT \t\t: " << DATA_BLOCK_READ_COUNT << endl
		<< "DATA READ CACHE HITS \t\t: " << DATA_BLOCK_READ_CACHE_HIT << endl
		<< "DATA READ CACHE HITS LDC \t\t: " << DATA_BLOCK_READ_CACHE_HIT_LDC << endl
		<< "DATA READ CACHE HITS INDIRECT \t\t: " << DATA_BLOCK_READ_CACHE_HIT_INDIRECT << endl
		<< "DATA READ CACHE MISS \t\t: " << DATA_BLOCK_READ_CACHE_MISS << endl
		<< "DATA READ CACHE MISS LDC \t\t: " << DATA_BLOCK_READ_CACHE_MISS_LDC << endl
		<< "DATA READ CACHE MISS INDIRECT \t\t: " << DATA_BLOCK_READ_CACHE_MISS_INDIRECT << endl
		<< "DATA WRITE COUNT \t\t: " << DATA_BLOCK_WRITE_COUNT << endl
		<< "DATA WRITE CACHE HITS \t\t: " << DATA_BLOCK_WRITE_CACHE_HIT << endl
		<< "DATA WRITE CACHE MISS \t\t: " << DATA_BLOCK_WRITE_CACHE_MISS << endl
		<< "DATA BLOCK EFFECTIVE READS \t: " << (DATA_BLOCK_READ_COUNT - DATA_BLOCK_READ_CACHE_HIT) << endl
		<< "DATA BLOCK EFFECTIVE WRITES \t: " << DATA_BLOCK_EFFECTIVE_WRITES << endl
		<< "DATA BLOCK FLUSH WRITES \t: " << DATA_BLOCK_FLUSH_WRITES << endl;


	//cout << "\n\nTotal records processed \t\t\t: " << trace[tno]->getCurCount() << endl;
	cout << "\n\nTotal Blocks \t\t\t: " << md->bt->keycount << endl
		<< "Unique blocks identified \t: " << md->pb->keycount << endl
		<< "Unique blocks identified (Allocated) \t: " << UNIQUE_BLOCK_COUNT << endl;
	
	md->displayStatistics();

	display_io_response();
	//display_iwrite_response();

	//cout << "\n\nLBA BTree index \n";
	//bt.displayStatistics();

	//cout << "\n\nPBA BTree index \n";
	//pb.displayStatistics();
	
	//cout << "\n\nBucket statistics\n";
	//bkt->displayStatistics();
	//cout << "\n\nHash Bucket statistics\n";
	//hbkt->displayStatistics();


	cout << "\n\nData cache statistics \n";
	dc.displayStatistics();
	//ldc.displayStatistics();

	//cout << "\n\nEFFECTIVE META DATA OPERATION COUNTS\n\n";
	//cout << "LBT SEARCH \t\t\t: " << bt.BTSEARCH << endl
		//<< "LBT INSERT \t\t\t: " << bt.BTINSERT << endl
		//<< "LBT DELETE \t\t\t: " << bt.BTDELETE << endl
		//<< "LBT UPDATE \t\t\t: " << bt.BTUPDATE << endl
		//<< "PBT SEARCH \t\t\t: " << pb.BTSEARCH << endl
		//<< "PBT INSERT \t\t\t: " << pb.BTINSERT << endl
		//<< "PBT DELETE \t\t\t: " << pb.BTDELETE << endl
		//<< "PBT UPDATE \t\t\t: " << pb.BTUPDATE  << endl
		//<< "BKT SEARCH \t\t\t: " << bkt->BKTSEARCH << endl
		//<< "BKT INSERT \t\t\t: " << bkt->BKTINSERT << endl
		//<< "BKT DELETE \t\t\t: " << bkt->BKTDELETE << endl
		//<< "BKT UPDATE \t\t\t: " << bkt->BKTUPDATE << endl
		//<< "HBKT SEARCH \t\t\t: " << hbkt->BKTSEARCH << endl
		//<< "HBKT INSERT \t\t\t: " << hbkt->BKTINSERT << endl
		//<< "HBKT DELETE \t\t\t: " << hbkt->BKTDELETE << endl
		//<< "HBKT UPDATE \t\t\t: " << hbkt->BKTUPDATE  << endl
		//<< "Total SEARCH \t\t\t\t: " << bt.BTSEARCH + pb.BTSEARCH + bkt->BKTSEARCH + bkt->BKTSEARCH << endl
		//<< "Total INSERT \t\t\t\t: " << bt.BTINSERT + pb.BTINSERT + bkt->BKTINSERT + bkt->BKTINSERT << endl
		//<< "Total DELETE \t\t\t\t: " << bt.BTDELETE + pb.BTDELETE + bkt->BKTDELETE + bkt->BKTDELETE << endl
		//<< "Total UPDATE \t\t\t\t: " << bt.BTUPDATE + pb.BTUPDATE + bkt->BKTUPDATE + bkt->BKTUPDATE << endl << endl;

/*
	float rr_ratio, rw_ratio, wr_ratio, ww_ratio;
	float err_ratio, erw_ratio, ewr_ratio, eww_ratio;
	//float READ_RESPONSE_TIME, WRITE_RESPONSE_TIME;
	//float EFFECTIVE_READ_RESPONSE_TIME, EFFECTIVE_WRITE_RESPONSE_TIME;

	rr_ratio = ((float)(bkt->BUCKET_READ_COUNT_READ + 
			bkt->bi->BT_BTNODE_READCOUNT_READ + 
			hbkt->HBUCKET_READ_COUNT_READ + 
			bt.BT_BTNODE_READCOUNT_READ +
			pb.BT_BTNODE_READCOUNT_READ)) / DATA_BLOCK_READ_COUNT;

	rw_ratio = ((float)(bkt->BUCKET_WRITE_COUNT_READ + 
			bkt->bi->BT_BTNODE_WRITECOUNT_READ + 
			hbkt->HBUCKET_WRITE_COUNT_READ + 
			bt.BT_BTNODE_WRITECOUNT_READ +
			pb.BT_BTNODE_WRITECOUNT_READ)) / DATA_BLOCK_READ_COUNT;

	wr_ratio = ((float)(bkt->BUCKET_READ_COUNT_WRITE + 
			bkt->bi->BT_BTNODE_READCOUNT_WRITE + 
			hbkt->HBUCKET_READ_COUNT_WRITE + 
			bt.BT_BTNODE_READCOUNT_WRITE +
			pb.BT_BTNODE_READCOUNT_WRITE)) / DATA_BLOCK_WRITE_COUNT;

	ww_ratio = ((float)(bkt->BUCKET_WRITE_COUNT_WRITE + 
			bkt->bi->BT_BTNODE_WRITECOUNT_WRITE + 
			hbkt->HBUCKET_WRITE_COUNT_WRITE + 
			bt.BT_BTNODE_WRITECOUNT_WRITE + 
			pb.BT_BTNODE_WRITECOUNT_WRITE)) / DATA_BLOCK_WRITE_COUNT;

	//cout << " rr_ratio \t: " << rr_ratio << "\trw_ratio \t: " << rw_ratio << endl
		//<< " wr_ratio \t: " << wr_ratio << "\tww_ratio \t: " << ww_ratio << endl
		//<< " DATA Block read count \t: " << DATA_BLOCK_READ_COUNT << endl
	       	//<< " DATA Block write count \t: " << DATA_BLOCK_WRITE_COUNT  << endl;

	err_ratio = (((float)((bkt->BUCKET_READ_COUNT -  
			bkt->BUCKET_READ_CACHEHIT) + 
			(bkt->bi->BT_BTNODE_READCOUNT -   
			bkt->bi->BT_BTNODE_READ_CACHE_HIT) + 
			(hbkt->HBUCKET_READ_COUNT - 
			hbkt->HBUCKET_READ_CACHEHIT) +
			(bt.BT_BTNODE_READCOUNT -   
			bt.BT_BTNODE_READ_CACHE_HIT) + 
			(pb.BT_BTNODE_READCOUNT -   
			pb.BT_BTNODE_READ_CACHE_HIT))) * rr_ratio) / 
			((rr_ratio + wr_ratio) * DATA_BLOCK_READ_COUNT);
	ewr_ratio = (((float)((bkt->BUCKET_READ_COUNT -
			bkt->BUCKET_READ_CACHEHIT) + 
			(bkt->bi->BT_BTNODE_READCOUNT - 
			bkt->bi->BT_BTNODE_READ_CACHE_HIT) + 
			(hbkt->HBUCKET_READ_COUNT - 
			hbkt->HBUCKET_READ_CACHEHIT) + 
			(bt.BT_BTNODE_READCOUNT - 
			bt.BT_BTNODE_READ_CACHE_HIT) + 
			(pb.BT_BTNODE_READCOUNT - 
			pb.BT_BTNODE_READ_CACHE_HIT))) * wr_ratio) / 
			((rr_ratio + wr_ratio) * DATA_BLOCK_WRITE_COUNT);
	erw_ratio = (((float)(bkt->BUCKET_EFFECTIVE_WRITES + 
			bkt->bi->BT_BTNODE_EFFECTIVE_WRITES + 
			hbkt->HBUCKET_EFFECTIVE_WRITES + 
			bt.BT_BTNODE_EFFECTIVE_WRITES + 
			pb.BT_BTNODE_EFFECTIVE_WRITES)) * rw_ratio) / 
			((rw_ratio + ww_ratio) * DATA_BLOCK_READ_COUNT);
	eww_ratio = (((float)(bkt->BUCKET_EFFECTIVE_WRITES + 
			bkt->bi->BT_BTNODE_EFFECTIVE_WRITES + 
			hbkt->HBUCKET_EFFECTIVE_WRITES + 
			bt.BT_BTNODE_EFFECTIVE_WRITES + 
			pb.BT_BTNODE_EFFECTIVE_WRITES)) * ww_ratio) / 
			((rw_ratio + ww_ratio) * DATA_BLOCK_WRITE_COUNT);
	cout << "Meta data reads " << rr_ratio << ", writes " << rw_ratio << " / Data block read\n\n";
	cout << "Meta data reads " << wr_ratio << ", writes " << ww_ratio << " / Data block write\n\n";

	cout << "Effective Meta data reads " << err_ratio << ", writes " << erw_ratio << " / Data block read\n\n";
	cout << "Effective Meta data reads " << ewr_ratio << ", writes " << eww_ratio << " / Data block write\n\n";

	cout << "\n\nExcluding FLUSH writes\n\n";

	erw_ratio = (((float)(bkt->BUCKET_EFFECTIVE_WRITES -
			bkt->BUCKET_FLUSH_WRITES +
			bkt->bi->BT_BTNODE_EFFECTIVE_WRITES - 
			bkt->bi->BT_BTNODE_FLUSH_WRITES + 
			hbkt->HBUCKET_EFFECTIVE_WRITES - 
			hbkt->HBUCKET_FLUSH_WRITES + 
			bt.BT_BTNODE_EFFECTIVE_WRITES -
			bt.BT_BTNODE_FLUSH_WRITES + 
			pb.BT_BTNODE_EFFECTIVE_WRITES -
			pb.BT_BTNODE_FLUSH_WRITES)) * rw_ratio) / 
			((rw_ratio + ww_ratio) * DATA_BLOCK_READ_COUNT);
	eww_ratio = (((float)(bkt->BUCKET_EFFECTIVE_WRITES - 
			bkt->BUCKET_FLUSH_WRITES +
			bkt->bi->BT_BTNODE_EFFECTIVE_WRITES - 
			bkt->bi->BT_BTNODE_FLUSH_WRITES + 
			hbkt->HBUCKET_EFFECTIVE_WRITES - 
			hbkt->HBUCKET_FLUSH_WRITES + 
			bt.BT_BTNODE_EFFECTIVE_WRITES -
			bt.BT_BTNODE_FLUSH_WRITES + 
			pb.BT_BTNODE_EFFECTIVE_WRITES -
			pb.BT_BTNODE_FLUSH_WRITES)) * rw_ratio) / 
			((rw_ratio + ww_ratio) * DATA_BLOCK_WRITE_COUNT);
	//cout << "Meta data reads " << rr_ratio << ", writes " << rw_ratio << " / Data block read\n\n";
	//cout << "Meta data reads " << wr_ratio << ", writes " << ww_ratio << " / Data block write\n\n";

	cout << "Effective Meta data reads " << err_ratio << ", writes " << erw_ratio << " / Data block read\n\n";
	cout << "Effective Meta data reads " << ewr_ratio << ", writes " << eww_ratio << " / Data block write\n\n";
*/
	cout << "SEGCOUNT3 : " << SEGCOUNT3 << " SEGLENGTH3 : " << SEGLENGTH3 << " AVG SEGMENT LENGTH : " << (SEGLENGTH3 / SEGCOUNT3) << endl;
	//displayWriteReqBufMemStat();

	//updateBitMap();
	//dsim.close();
	trace[tno]->closeTrace();	

	return 0;
}


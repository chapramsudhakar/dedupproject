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

void md5(unsigned char *buf, unsigned int nbytes, unsigned char hash[16]);

void cacheMemStats(void);

string dcstr = "datacache";
#ifdef	DARCP
#ifdef	HOME
DARC dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE, 512);
#else
DARC dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE, 4096);
#endif
#else
LRU dc(dcstr.c_str(), DATA_CACHE_LIST, DATA_CACHE_SIZE);
#endif

IOTraceReader *trace[10];

string dummyfile="/dev/null";
fstream dummy;

unsigned long int prevblock[2];

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
//unsigned long int DISK_READ_REQ_COUNT[1028][1000];
//unsigned long int DISK_WRITE_REQ_COUNT[1028][1000];
//unsigned long int DISK_IWRITE_REQ_COUNT[1028][1000];
unsigned long long int READ_REQ_COUNT = 0;
unsigned long long int WRITE_REQ_COUNT = 0;

unsigned long long int VERTEX_CREATE_COUNT = 0;
unsigned long long int VERTEX_FIND_COUNT = 0;
unsigned long long int VERTEX_REMOVE_COUNT = 0;
unsigned long long int VERTEX_PARENT_CHANGE_COUNT = 0;

double SEGCOUNT3 = 0.0;
double SEGLENGTH3 = 0.0;

int OPERATION;

//MDMaps *md;

#ifdef	WEB
#define	MAX_SEG_SIZE_USED	(MAX_SEG_SIZE / 2)
#else
#define	MAX_SEG_SIZE_USED	MAX_SEG_SIZE
#endif

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

void update_statistics(double var[], double mean[],
	double minimum[], double maximum[],
	unsigned long int  N[], int seglen, unsigned int microsec) {
	unsigned long int n, i, count;

	count = ((seglen + 1023) / 1024);
	seglen /= count;
	microsec /= count;	

	for (i=0; i<count; i++) {
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

	SEGCOUNT3++;	// Last segment
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
				//cout << "pba " << pba2 
					//<< " dc.count " << dc.getCount() 
					//<< " wcount " << wcount << endl;	
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

void readFileData(unsigned int pba[], unsigned int seg_size, unsigned long long int ts, unsigned long long int incr) {
	unsigned long long int rtime;
	unsigned int i;
	unsigned int pbastart, count;

	// Search for the data blocks segment by segment in the cache
	// with the representative pba number
	i = 0;
	rtime = ts;

	while (i < seg_size) {
		pbastart = pba[i];
		count = 1;
		i++;

		while ((i < seg_size) && (pba[i] == (pba[i-1] + 1))) {
			count++;
			i++;
		}

		readSegmentDC(pbastart, count, rtime, incr);
		rtime += (incr * count);
	}
}

void writeFileData(unsigned int pba[], unsigned int seg_size, unsigned long long int ts, unsigned long long int incr) {
	unsigned long long int wtime;
	unsigned int i;
	unsigned int pbastart, count;

	// Search for the data blocks segment by segment in the cache
	// with the representative pba number
	i = 0;
	wtime = ts;

	while (i < seg_size) {
		pbastart = pba[i];
		count = 1;
		i++;

		while ((i < seg_size) && (pba[i] == (pba[i-1] + 1))) {
			count++;
			i++;
		}

		writeSegmentDC(pbastart, count, wtime, incr);
		wtime += (incr * count);
	}
}

// Get lba to pba map values from IBTree
void getLba2PbaMaps(unsigned int lbastart, unsigned int nb,
	IBTree *ibt, unsigned int pba[]) {
	unsigned long long int lba;
	unsigned int i;

	lba = lbastart;

	for (i=0; i<nb; i++) {
		ibt->searchValWrap(lba, &pba[i]);
		lba++;
	}
}

// Determines whether the blocks of file are part of
// a large file. If YES returns starting LBA of the file (non-zero)
// Otherwise returns zero.
/*unsigned int isPartOfOldFile(struct FileBlocks *fb, MDMaps *m) {
	unsigned int fl, fnb;
	unsigned int l, r;

	l = fb->fb_lbastart;
	r = l + fb->fb_nb;

	m->files->searchRangeWrap(l, r, &fl, &fnb);

	// Return the file's starting lba fl. (If not found fl=0)
	return fl;
}*/

// Delete Small file from file table and decrement reference counts 
// of the blocks in hash bucket
/*void deleteSmallFile(unsigned int sfile, unsigned int nb, MDMaps *m) {
	unsigned int i;
	unsigned int hbno, pba;
	CacheEntry *c1;


	// Find the corresponding pba's and bucket numbers
	// and decrement their reference counts
	for (i=0; i<nb; i++) {
		m->bt->searchValWrap(sfile+i, &pba);
		if (pba != 0) 
			m->pb->searchValWrap(pba, &hbno);
		else {
			cout << "Error: deleteSmallFile " << sfile+i 
				<< " block map not found " << endl;
			continue;
		}

		if (m->hbkt->decrementRefValWrap(hbno, pba) == 0) {
			m->pb->deleteValWrap(pba);
			// Free the block, and make
			// the cache entry reusable
			freeBlock(pba);
			c1 = dc.searchCache(pba);
			if (c1 != NULL) {
				dc.deleteEntry(c1);
				cefree(c1);
			}
			UNIQUE_BLOCK_COUNT--;
		}
	}

	// Finally delete file entry
	m->files->deleteValWrap(sfile);

	return;
}*/

// Update the superchunk of the large file that contains the 
// blocks pointed by fb.
//void updateLargeFile(struct FileBlocks *fb, MDMaps *m, unsigned int lfile) {
//}

// Small file deduplication, for all H/U/L types
// Assumes that this small file blocks are not part of any larger file 
void processSmallFile(struct FileBlocks *fb, MDMaps *m,
	unsigned int pba[]) {
	unsigned int bpba[SMALL_FILE_BLKS];	
	unsigned int refcount;
	unsigned int hbno;
	unsigned int i;
	HashList *hl;

#ifdef	HOME
	clock_add(fb->fb_nb * 4);
	t0_hash += (fb->fb_nb * 4);
#else
	clock_add(fb->fb_nb * 30);
	t0_hash += (fb->fb_nb * 30);
#endif

	// Find the hash bucket corresponding to the hash
	// Search for the exact matching hash
	for (i=0, hl = fb->fb_hlist; i<fb->fb_nb; i++, hl = hl->hl_next) {
		m->hbkt->searchValWrap(hl->hl_hash, &bpba[i], &refcount);
			
		// If necessary insert the hash
		// Appropriately increment/decrement pba reference count
		// insert/update/delete the entries in old buckets and 
		// lba-pba-bno btree
		hbno = (hl->hl_hash[0] | (hl->hl_hash[1] << 8)) + 1;
		if (bpba[i] == 0) { 
			// This fp is not found in the hash
			// Add new entry
			pba[i] = allocBlocks(1);
			assert(pba[i] > 0);
			UNIQUE_BLOCK_COUNT ++;
			m->hbkt->insertValWrap(hbno, pba[i], hl->hl_hash, 1);
			m->bt->insertValWrap(fb->fb_lbastart+i, pba[i]);
			m->pb->insertValWrap(pba[i], hbno);
		}
		else {
			pba[i] = bpba[i];
			// Increment the pba reference count and add lba entry
			m->hbkt->incrementRefValWrap(hbno, pba[i]);
			m->bt->insertValWrap(fb->fb_lbastart+i, pba[i]);
		}
	}
}

// Large file deduplication, for H/U types
void processHULargeFile(struct FileBlocks *fb, MDMaps *m,
	unsigned int pba[]) {
	unsigned int clba;
	HashList *hl1;
	unsigned int i, j, bn;
	unsigned char *mhash;
	unsigned int count;
	unsigned int bnonew, seg_pba;
	unsigned int bpba[MAX_DEDUP_SEG_SIZE], bbnos[MAX_DEDUP_SEG_SIZE];
	unsigned int brefcounts[MAX_DEDUP_SEG_SIZE];

#ifdef	HOME
	clock_add(fb->fb_nb * 4);
	t0_hash += (fb->fb_nb * 4);
#else
	clock_add(fb->fb_nb * 30);
	t0_hash += (fb->fb_nb * 30);
#endif

	// Divide the file into MAX_SEG_SIZE segments
	hl1 = fb->fb_hlist;
	clba = fb->fb_lbastart;
	for (i=0; i<fb->fb_nb; i=i+MAX_DEDUP_SEG_SIZE) {
		// Find min hash for the next segment
		count = ((i+MAX_DEDUP_SEG_SIZE) <= fb->fb_nb) ? MAX_DEDUP_SEG_SIZE 
			: (fb->fb_nb - i);
		mhash = minHash3(hl1, count);

		// Locate the similar bin/bucket
		bn = m->bkt->findBucket(mhash);

		if (bn == 0) {
			// New bucket is to be created
			// Add this segment to new bucket
			bnonew = m->bkt->addNewBucket(mhash);
			assert(bnonew > 0);

			// Allocate new blocks
			seg_pba = allocBlocks(count);
			assert(seg_pba > 0);
			UNIQUE_BLOCK_COUNT += count;

			// Add lba-pba-bno new entries
			for (j=0; j<count; j++) {
				pba[i+j] = seg_pba + j;
				m->bkt->insertValWrap(bnonew+HASH_BUCK_COUNT, pba[i+j], hl1->hl_hash, 1);
				m->bt->insertValWrap(clba+j, pba[i+j]);
				m->pb->insertValWrap(pba[i+j], bnonew+HASH_BUCK_COUNT);
				hl1 = hl1->hl_next;
			}
		}
		else {
			// Deduplicate the segment and store the unique blocks
			// Update metadata maps
			for (j=0; j<count; j++)
				bpba[j] = bbnos[j] = 0;
			m->bkt->searchHashesWrap(bn+HASH_BUCK_COUNT, hl1, count, bpba, bbnos, brefcounts);
			// First add missing hashes 
			for (j=0; j<count; j++) {
				if (bpba[j] == 0) {
					// Hash not found in the bucket
					pba[i+j] = allocBlocks(1);
					assert(pba[i+j] > 0);
					UNIQUE_BLOCK_COUNT ++;
				}
			}

			for (j=0; j<count; j++) {
				if (bpba[j] != 0) {
					m->bkt->incrementRefValWrap(bbnos[j], bpba[j]);
					pba[i+j] = bpba[j];
				}
				else {
					m->bkt->insertValWrap(bn+HASH_BUCK_COUNT, pba[i+j], hl1->hl_hash, 1);
					m->pb->insertValWrap(pba[i+j], bn+HASH_BUCK_COUNT);
				}
				m->bt->insertValWrap(clba+j, pba[i+j]);
				hl1 = hl1->hl_next;
			}
		}
		clba += count;
	}
}

void addNewFile(unsigned char filehash[], unsigned int lba, 
		unsigned int nblocks, MDMaps *m, 
		unsigned int pba[]) {
	unsigned int fbpba, fbnb, fbnext, fbent, fbentstart;
	unsigned int i, k, nb, stpba;

	// Not present, so add the entry
	k = 0;
	fbentstart = fbent = m->fbl->getFreeFbe();
	fbpba = 0;
	fbnb = 0;
	fbnext = 0;

	while  (k<nblocks) {
		nb = (k+16 <= nblocks) ? 16 : (nblocks - k);
		stpba = allocBlocks(nb);
		assert(stpba > 0);

		if (fbpba == 0) {
			// First segment
			fbpba = stpba;
			fbnb = nb;
		}
		else if ((fbpba + fbnb) == stpba) {
			// Adjacent to current segment, merge
			fbnb += nb;
		}
		else {
			// New segment start
			fbnext = m->fbl->getFreeFbe();

			// Save old segment
			m->fbl->fbePutEntry(fbent, fbpba, fbnb, fbnext);

			fbent = fbnext;
			fbpba = stpba;
			fbnb = nb;
			fbnext = 0;
		}

		for (i=0; i<nb; i++) {
			m->bt->insertValWrap(lba+k+i, stpba+i);
			pba[k+i] = stpba+i;
		}
		k += nb;
	}

	// Save the last segment
	m->fbl->fbePutEntry(fbent, fbpba, fbnb, fbnext);

	UNIQUE_BLOCK_COUNT += nblocks;
	m->fhbt->insertValWrap(filehash, fbentstart, nblocks, 1);
}

void addLbaMap(unsigned int lba, unsigned int nb, unsigned int stent,
		MDMaps *m) {
	unsigned int i, k;
	unsigned int fbpba, fbnb, fbnext, fbe;

	k = 0;
	fbe = stent;
	while (fbe != 0) {
		m->fbl->fbeGetEntry(fbe, &fbpba, &fbnb, &fbnext);
		
		for (i=0; i<fbnb; i++) 
			m->bt->insertValWrap(lba+k+i, fbpba+i);
		k += fbnb;
		fbe = fbnext;
	}
	if (k != nb) 
		cout << "Error: addLbaMap lba " << lba << " nb " << nb 
			<< " k " << k << endl;
}

// Large file deduplication, for L type
void processWholeFile(struct FileBlocks *fb, MDMaps *m,
	unsigned int pba[]) {
	static unsigned int maxsize = 0;
	static unsigned char *buf = NULL;
	unsigned int i, size;
	HashList *hl;
	unsigned char filehash[16];
	unsigned int stent, nblks, refs;
	//unsigned int fbpba, fbnb, fbnext, fbent, fbentstart;

#ifdef	HOME
	clock_add(fb->fb_nb * 4);
	t0_hash += (fb->fb_nb * 4);
#else
	clock_add(fb->fb_nb * 30);
	t0_hash += (fb->fb_nb * 30);
#endif

	// Compute whole file hash, using hash of hashes(approximately)
	size = fb->fb_nb * 16;
	if (maxsize < size) {
		if (buf != NULL) free(buf);
		maxsize = size;
		buf = (unsigned char *) malloc(maxsize);
		assert(buf != NULL);
	}

	hl = fb->fb_hlist;
	for (i=0; i<fb->fb_nb; i++) {
		memcpy(buf+(i*16), hl->hl_hash, 16);
		hl = hl->hl_next;
	}
	md5(buf, size, filehash);

	// Search for the whole file hash value	
	m->fhbt->searchValWrap(filehash, &stent, &nblks, &refs);
	if (stent == 0) {
		addNewFile(filehash, fb->fb_lbastart, fb->fb_nb, m, pba);
	}
	else {
		// Check if exact match is there?
		if (nblks == fb->fb_nb) {
			m->fhbt->incrementRefCountWrap(filehash);
			addLbaMap(fb->fb_lbastart, fb->fb_nb, stent, m);
			//for (i=0; i<fb->fb_nb; i++)
				//m->bt->insertValWrap(fb->fb_lbastart+i, stpba+i);
		}
		else {
			// Otherwise it cannot be handled now here????
			cout << "Error: Two files having same hash value, \n"
				<< "but different data, this cannot be handled now!" << endl;
			cout << "old : (" << stent << ", " << nblks 
				<< ", refs " << ")\n" << "new : (" 
				<< fb->fb_lbastart << ", " << fb->fb_nb 
				<< ", .. )" << endl;
		}
	}
}

// Process read file 
void processReadFile(struct FileBlocks *fb, MDMaps *m,
	unsigned int pba[]) {
	unsigned int clba;
	unsigned int i;

	// Find all logical to physical block maps
	clba = fb->fb_lbastart;
	for (i=0; i<fb->fb_nb; i++) 
		m->bt->searchValWrap(clba+i, &pba[i]);
}

/*
// Full deduplication, every block wise
void processAllBlocks(struct FileBlocks *fb, MDMaps *m, StorageNode *sn,
	unsigned long long int pba[]) {
	unsigned long long int opba[fb->fb_nb];
	int i;
	unsigned long long int pba2, lba;
	HashList *hl;

	getLba2PbaMaps(fb->fb_lbastart, fb->fb_nb, m->bt, opba);

	// If the operation is write try to find the duplicate
	// physical block addresses within finger print cache.
	// If not present search the hash index. If there also
	// not found then add new entry to hash index.
	// Search the lba index also. If exists and finger
	// print are different, then update the finger print.
	// If not present then add a new entry.

#ifdef  HOME
	clock_add(fb->fb_nb*8);
#else
	clock_add(fb->fb_nb*32);
#endif

	hl = fb->fb_hlist;
	for (i=0; i<fb->fb_nb; i++) {
		lba = fb->fb_lbastart + i;

		// Search for the duplicate hash in hash index
		m->hbt->searchValWrap(hl->hl_hash, &pba[i]);

		if (pba[i] == 0) {
			// New one, Allocate a block and add hash entry
			pba[i] = sn->bmap->allocBlocks(1);
			assert(pba[i] > 0);
			UNIQUE_BLOCK_COUNT++;
			m->hbt->insertValWrap(hl->hl_hash, pba[i]);
		}

		if (opba[i] == 0) {
			m->bt->insertValWrap(lba, pba[i]);
			m->pbaref->incrementPbaRef(pba[i], hl->hl_hash);
		}
		else if (pba[i] != opba[i]) {
			retval = m->pbaref->decrementPbaRef(opba[i]);

			if (retval == 0) {
				m->hbt->deleteValWrap(ohash);
				sn->bmap->freeBlock(opba[i]);
				UNIQUE_BLOCK_COUNT--;
				c = sn->dc->searchCache(opba[i]);
				if (c != NULL) {
					sn->dc->deleteEntry(c);
					cefree(c);
				}
			}

			m->bt->updateValWrap(lba, pba[i]);
			m->pbaref->incrementPbaRef(pba[i], hl->hl_hash);
		}
	}
}
*/


void processReadTrace(int tno, MDMaps *m) {
	IOTraceReader *rtrace;
	//int op; 
	unsigned int pbasize = 0;
	unsigned int *pba = NULL;
	//unsigned int lastFlushTime = 0, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	struct FileBlocks fb;
	//int count;
	int retval; 
	unsigned int ind;
	unsigned long dtime, overhead;

	reccount100000 = 1;
	// Open the read operations trace
	if (tno == 2)
		rtrace = new IOTraceReaderHome();
	else 	rtrace = new IOTraceReader4K();

	if (rtrace->openTrace(frnames[tno], fbsize[tno]) != 0) {
		perror("IO Trace opening failure:");
		exit(0);
	}

	fb.fb_hlist = NULL;

	while (1) {
		retval = rtrace->readFile(&fb);

		if (retval == 0) break;	// End of the trace is reached

		ts1 = fb.fb_ts1;
		ts2 = fb.fb_ts2;
		//op = fb.fb_rw;

		READ_REQ_COUNT++;
		OPERATION = OPERATION_READ;
		DATA_BLOCK_READ_COUNT += fb.fb_nb;
#ifndef	HOME
		fb.fb_nb /= 8;
		fb.fb_lbastart /= 8;
#endif
		if (fb.fb_nb > 1)
			incr = (ts2 - ts1) / (fb.fb_nb - 1);
		else	incr = 2;

		curTime = ts1;
		if (incr <= 0) incr = 2;

		if (pbasize < fb.fb_nb) {
			if (pba != NULL) free(pba);
			pbasize = ((fb.fb_nb + 255)/256) * 256;
			pba = (unsigned int *)malloc(pbasize * sizeof(unsigned int));
			assert(pba != NULL);
		}

		clock_start();
		disk_start();

		processReadFile(&fb, m, pba);

		readFileData(pba, fb.fb_nb, ts1, incr);

		overhead = clock_stop();
		dtime = disk_stop();

		ind = dtime * 1000 + overhead;
#ifdef  HOME
		update_statistics(r_var, r_mean, r_min, r_max, r_n, fb.fb_nb, ind);
#else
		update_statistics(r_var, r_mean, r_min, r_max, r_n, fb.fb_nb*8, ind);
#endif

		if (rtrace->getCurCount() >= (reccount100000 * 1000000)) {
			cout << rtrace->getCurCount() << "  number of records processed" << endl;
			reccount100000++;
		}

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
			darcInterval++;
                }
#endif
	}
	cout <<"\n\nTotal read records processed \t\t\t: " << rtrace->getCurCount() << endl;
}

int main(int argc, char** argv) {
	int i; 
	int op; 
	unsigned int pbasize = 0;
	unsigned int *pba = NULL;
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int ts1, ts2, incr;
	unsigned int twritecount;
	int retval; 
	unsigned int ind;
	unsigned long dtime, overhead;
	int tno = 0;
	struct FileBlocks fb;
	//int count;
	//unsigned long long int metadatasize;
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
#ifdef	LINUX
	tno = 3;
#endif
#ifdef	BOOKPPT
	tno = 4;
#endif
#ifdef	VIDIMG
	tno = 5;
#endif

	cout << "WSS " << WSS << endl;
	cout << "B512M_1 " << B512M_1 << endl;
	cout << "B512M_2 " << B512M_2 << endl;
	cout << "B512MCOUNT " << B512MCOUNT << endl;
	initCacheMem();
	//exit(0);

	md = new MDMaps("blockidx.dat", "pbaidx.dat", "hashidx.dat",
			"filehashidx.dat", "buckets.dat", "hashbuckets.dat",
			"pbaref.dat", "fblist.dat", DEDUPAPP);

	dummy.open(dummyfile.c_str(), ios::in | ios::out | ios::binary);
	openBitMap();

	for (i=0; i<6; i++) {
		if (i == 2)
			trace[i] = new IOTraceReaderHome();
		else 	trace[i] = new IOTraceReader4K();

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
	fb.fb_hlist = NULL;
	//unsigned int x;
	while (1) {
		retval = trace[tno]->readFile(&fb);

		if (retval == 0) break;	// End of the trace is reached

		//cout << "file " << x << " : " << fb.fb_nb << " == " << trace[tno]->getCurCount() << endl;
		//x++; continue;
		ts1 = fb.fb_ts1;
		ts2 = fb.fb_ts2;
		op = fb.fb_rw;

		if (op == OP_READ) {
			READ_REQ_COUNT++;
			OPERATION = OPERATION_READ;
			DATA_BLOCK_READ_COUNT += fb.fb_nb;
		}
		else {
			WRITE_REQ_COUNT++;
			OPERATION = OPERATION_WRITE;
			DATA_BLOCK_WRITE_COUNT += fb.fb_nb;
		}
#ifndef	HOME
		fb.fb_nb /= 8;
		fb.fb_lbastart /= 8;
#endif
		if (fb.fb_nb > 1)
			incr = (ts2 - ts1) / (fb.fb_nb - 1);
		else	incr = 2;

		curTime = ts1;
		if (incr <= 0) incr = 2;

		if (pbasize < fb.fb_nb) {
			if (pba != NULL) free(pba);
			pbasize = ((fb.fb_nb + 255)/256) * 256;
			pba = (unsigned int *)malloc(pbasize * sizeof(unsigned int));
			assert(pba != NULL);
		}

		clock_start();
		disk_start();

		if (fb.fb_nb <= SMALL_FILE_BLKS) {
			// Small file
			processSmallFile(&fb, md, pba);
		}
		else {
			// Large file
			if (ftype[tno] == LTYPE)
				processWholeFile(&fb, md, pba);
			else	processHULargeFile(&fb, md, pba);
		}
		writeFileData(pba, fb.fb_nb, ts1, incr);

		overhead = clock_stop();
		dtime = disk_stop();

		ind = dtime * 1000 + overhead;
#ifdef  HOME
		update_statistics(w_var, w_mean, w_min, w_max, w_n, fb.fb_nb, ind);
#else
		update_statistics(w_var, w_mean, w_min, w_max, w_n, fb.fb_nb*8, ind);
#endif

		//cout << "Step 1" << endl;
		countSegmentLength(pba, fb.fb_nb);

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
			updateBitMap();
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
			lastFlushTime = (ts2 / 1000000);

			overhead = clock_stop();
			dtime = disk_stop();

			ind = dtime * 1000 + overhead; // Microseconds
			w_flush += ind;			
		}
#endif	

#ifdef	FLUSH_TIME_75PERCENT
		if ((dc.c_dcount * 100 / dc.c_count) > 75) {
			md->flushCache();
			updateBitMap();
			// Dummy writing ...
			twritecount = dc.flushCache(dummy, 0, DATADISK);
			DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
			DATA_BLOCK_FLUSH_WRITES += twritecount;
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
			darcInterval++;
                }
#endif

		// Garbage collection once in an hour
		//if (((ts2 / 1000000) - lastGCTime) > (60000 * 60)) {
			//gc();
			//lastGCTime = ts2 / 1000000;
		//}
		//cout << "Step 2" << endl;
	}

	clock_start();
	disk_start();

	md->flushCache();
	updateBitMap();

	twritecount = dc.flushCache(dummy, 0, DATADISK);
	DATA_BLOCK_EFFECTIVE_WRITES += twritecount;
	DATA_BLOCK_FLUSH_WRITES += twritecount;

        overhead = clock_stop();
        dtime = disk_stop();

        ind = dtime * 1000 + overhead; // Microseconds
        w_flush += ind;

        cout << "\n\nTotal write records processed \t\t\t: " << trace[tno]->getCurCount() << endl;

	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &tval);
	t2_clk = tval.tv_sec * 1000000 + (tval.tv_nsec / 1000);
	t2_disk0 = DISK_WRITE_TOT_TIME[0];
	t2_disk1 = DISK_READ_TOT_TIME[1] + DISK_WRITE_TOT_TIME[1];

	//======================================
	cout << "\n============ READ Trace ("
		<< frnames[tno] << ") processing =========\n\n";	
	processReadTrace(tno, md);

	//=======================================

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
		<< " KB\n"; // << "Meta data cache size \t: " 
; //		<< ((BUCKET_CACHE_SIZE + HBUCKET_CACHE_SIZE +
; //			BUCKETINDEX_CACHE_SIZE + (2 * BLOCKINDEX_CACHE_SIZE3) + 
; //			LBA2PBA_CACHE_SIZE + PBA2BUCK_CACHE_SIZE + 
; //			RID_CACHE_SIZE + WRB_CACHE_SIZE +
; //			HLPL_CACHE_SIZE + LPL_CACHE_SIZE) / (1024)) << " KB\n";

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
		<< "DATA READ CACHE MISS \t\t: " << DATA_BLOCK_READ_CACHE_MISS << endl
		<< "DATA WRITE COUNT \t\t: " << DATA_BLOCK_WRITE_COUNT << endl
		<< "DATA WRITE CACHE HITS \t\t: " << DATA_BLOCK_WRITE_CACHE_HIT << endl
		<< "DATA WRITE CACHE MISS \t\t: " << DATA_BLOCK_WRITE_CACHE_MISS << endl
		<< "DATA BLOCK EFFECTIVE READS \t: " << (DATA_BLOCK_READ_COUNT - DATA_BLOCK_READ_CACHE_HIT) << endl
		<< "DATA BLOCK EFFECTIVE WRITES \t: " << DATA_BLOCK_EFFECTIVE_WRITES << endl
		<< "DATA BLOCK FLUSH WRITES \t: " << DATA_BLOCK_FLUSH_WRITES << endl;


	//cout << "\n\nTotal records processed \t\t\t: " << trace[tno]->getCurCount() << endl;
	cout << "\n\nTotal Blocks (lba btree)\t\t\t: " << md->bt->keycount << endl
		<< "Unique blocks identified (pb btree)\t: " << md->pb->keycount << endl
		<< "Unique blocks identified (Allocated) \t: " << UNIQUE_BLOCK_COUNT << endl;

	md->displayStatistics();

	display_io_response();
	cout << "\n\nData cache statistics \n";
	dc.displayStatistics();

	cout << "SEGCOUNT3 : " << SEGCOUNT3 << " SEGLENGTH3 : " << SEGLENGTH3 << " AVG SEGMENT LENGTH : " << (SEGLENGTH3 / SEGCOUNT3) << endl;

	trace[tno]->closeTrace();	

	return 0;
}


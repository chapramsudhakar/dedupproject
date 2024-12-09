
// 
// All type of deduplication operations together HDS/FULL/APP/NATIVE
//
#include <iostream>
#include <fstream>
#include <assert.h>
#include <string.h>
#include "dedupoperations.h"
#include "storagenode.h"
#include "util.h"

#ifdef	WEB
#define	MAX_SEG_SIZE_USED	MAX_SEG_SIZE / 2
#else
#define	MAX_SEG_SIZE_USED	MAX_SEG_SIZE
#endif

void md5(unsigned char *buf, unsigned int nbytes, unsigned char hash[16]);
void update_statistics(double var[], double mean[],
        double minimum[], double maximum[],
        unsigned long int  N[], int seglen, unsigned int microsec);

extern fstream dummy;

double SEGCOUNT = 0.0;
double SEGLENGTH = 0.0;


void countSegmentLength(unsigned int pba[], unsigned int seg_size) {
	unsigned int i;
	int diff;

	SEGLENGTH += seg_size;

	// Already not existing
	i = 1;
	while (i<seg_size) {
		diff = pba[i] - pba[i-1];

		if (abs(diff) > 2)  
			SEGCOUNT += 1;
		
		i++;
	}

	SEGCOUNT += 1;	// Last segment
}

void readSegmentDC(StorageNode *sn, unsigned int pba, unsigned int count, 
		unsigned long long int ts, unsigned long long int incr) {
	unsigned int j, k;
	unsigned int pba1, pba2;
	unsigned int rtime = ts;
	unsigned int scount;
	CacheEntry *c;
	unsigned short cflags;
	int wcount;

	// Search for the whole segment?
	for (j=0; j<count; j++) {
		pba1 = pba + j;
		c = sn->dc->searchCache(pba1);
		if (c != NULL) {
			sn->DATA_BLOCK_READ_CACHE_HIT += 8;
			c->ce_refcount++;
			c->ce_rtime = rtime;
			sn->dc->c_rrefs++;
			rtime += incr;
			sn->curTime += incr;
#ifdef	DARCP
			sn->dc->repositionEntry(c);
#endif
		}
		else {
			// Find the sequence of blocks not in 
			// data cache, and issue single request
			scount = 1;
			pba2 = pba1+1;
			j++;

			while ((j < count) && ((c = sn->dc->searchCache(pba2)) == NULL)) {
				j++; scount++; pba2++;
			}
			j--; //Go back to the last block
		
			DiskRead(sn, sn->curTime, DATADISK, 8*pba1, 8*scount);
			sn->DATA_BLOCK_READ_CACHE_MISS += (8 * scount);
			cflags = CACHE_ENTRY_CLEAN;
			sn->curTime += (incr * scount);

			// Add missing data block entries to the cache
			for (k=0; k<scount; k++) {
				pba2 = pba1 + k;
				wcount = sn->dc->addReplaceEntry(dummy, pba2, NULL, 4096, rtime, 0, DATADISK, cflags, NULL, &c);
				sn->DATA_BLOCK_EFFECTIVE_WRITES += wcount;
			}

			rtime += (incr * scount);
		}
	}
}

void writeSegmentDC(StorageNode *sn, unsigned int pba, unsigned int count, 
		unsigned long long int ts, unsigned long long int incr) {
	unsigned int j;
	unsigned int pba1;
	unsigned int wtime = ts;
	CacheEntry *c;
	unsigned short cflags;
	int wcount;

	// Search for the whole segment?
	for (j=0; j<count; j++) {
		pba1 = pba + j;
		c = sn->dc->searchCache(pba1);
		if (c != NULL) {
			sn->DATA_BLOCK_WRITE_CACHE_HIT += 8;
			c->ce_refcount++;
			c->ce_wrefcount++;
			sn->dc->c_wrefs++;
			c->ce_wtime = wtime;
			if ((c->ce_flags & CACHE_ENTRY_DIRTY) == 0) {
				c->ce_flags |= CACHE_ENTRY_DIRTY;
				sn->dc->c_dcount++;
			}
#ifdef  DARCP
			sn->dc->repositionEntry(c);
#endif
		}
		else {
			cflags = CACHE_ENTRY_DIRTY;
			sn->DATA_BLOCK_WRITE_CACHE_MISS += 8;

			wcount = sn->dc->addReplaceEntry(dummy, pba1, NULL, 4096, 0, wtime, DATADISK, cflags, NULL, &c);
			sn->DATA_BLOCK_EFFECTIVE_WRITES += wcount;
		}
		wtime += incr;
		sn->curTime += incr;
	}
}

void readData(StorageNode *sn, unsigned int pba[], unsigned int seg_size, 
		unsigned long long int ts, unsigned long long int incr) {
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

		readSegmentDC(sn, pbastart, count, rtime, incr);
		rtime += (incr * count);
	}
}

void writeData(StorageNode *sn, unsigned int pba[], unsigned int seg_size, 
		unsigned long long int ts, unsigned long long int incr) {
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

		writeSegmentDC(sn, pbastart, count, wtime, incr);
		wtime += (incr * count);
	}
}




===================================
===================================
===================================
===================================
===================================
===================================
===================================
===================================

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


// Small file deduplication, for all H/U/L types
// Assumes that this small file blocks are not part of any larger file 
void processSmallFile(StorageNode *sn, struct FileBlocks *fb, 
	unsigned int pba[]) {
	unsigned int bpba[SMALL_FILE_BLKS];	
	unsigned int refcount;
	unsigned int hbno;
	unsigned int i;
	HashList *hl;

	clock_add(sn, fb->fb_nb * 30);
	sn->t0_hash += (sn, fb->fb_nb * 30);

	// Find the hash bucket corresponding to the hash
	// Search for the exact matching hash
	for (i=0, hl = fb->fb_hlist; i<fb->fb_nb; i++, hl = hl->hl_next) {
		sn->md->hbkt->searchValWrap(hl->hl_hash, &bpba[i], &refcount);
			
		// If necessary insert the hash
		// Appropriately increment/decrement pba reference count
		// insert/update/delete the entries in old buckets and 
		// lba-pba-bno btree
		hbno = (hl->hl_hash[0] | (hl->hl_hash[1] << 8)) + 1;
		if (bpba[i] == 0) { 
			// This fp is not found in the hash
			// Add new entry
			pba[i] = sn->bmap->allocBlocks(1);
			assert(pba[i] > 0);
			sn->UNIQUE_BLOCK_COUNT ++;
			sn->md->hbkt->insertValWrap(hbno, pba[i], hl->hl_hash, 1);
			sn->md->bt->insertValWrap(fb->fb_lbastart+i, pba[i]);
			sn->md->pb->insertValWrap(pba[i], hbno);
		}
		else {
			pba[i] = bpba[i];
			// Increment the pba reference count and add lba entry
			sn->md->hbkt->incrementRefValWrap(hbno, pba[i]);
			sn->md->bt->insertValWrap(fb->fb_lbastart+i, pba[i]);
		}
	}
}

// Large file deduplication, for H/U types
void processHULargeFile(StorageNode *sn, struct FileBlocks *fb, 
	unsigned int pba[]) {
	unsigned int clba;
	HashList *hl1;
	unsigned int i, j, bn;
	unsigned char *mhash;
	unsigned int count;
	unsigned int bnonew, seg_pba;
	unsigned int bpba[MAX_DEDUP_SEG_SIZE], bbnos[MAX_DEDUP_SEG_SIZE];
	unsigned int brefcounts[MAX_DEDUP_SEG_SIZE];

	clock_add(sn, fb->fb_nb * 30);
	sn->t0_hash += (fb->fb_nb * 30);

	// Divide the file into MAX_SEG_SIZE segments
	hl1 = fb->fb_hlist;
	clba = fb->fb_lbastart;
	for (i=0; i<fb->fb_nb; i=i+MAX_DEDUP_SEG_SIZE) {
		// Find min hash for the next segment
		count = ((i+MAX_DEDUP_SEG_SIZE) <= fb->fb_nb) ? MAX_DEDUP_SEG_SIZE 
			: (fb->fb_nb - i);
		mhash = minHash3(hl1, count);

		// Locate the similar bin/bucket
		bn = sn->md->bkt->findBucket(mhash);

		if (bn == 0) {
			// New bucket is to be created
			// Add this segment to new bucket
			bnonew = sn->md->bkt->addNewBucket(mhash);
			assert(bnonew > 0);

			// Allocate new blocks
			seg_pba = sn->bmap->allocBlocks(count);
			assert(seg_pba > 0);
			sn->UNIQUE_BLOCK_COUNT += count;

			// Add lba-pba-bno new entries
			for (j=0; j<count; j++) {
				pba[i+j] = seg_pba + j;
				sn->md->bkt->insertValWrap(bnonew+HASH_BUCK_COUNT, pba[i+j], hl1->hl_hash, 1);
				sn->md->bt->insertValWrap(clba+j, pba[i+j]);
				sn->md->pb->insertValWrap(pba[i+j], bnonew+HASH_BUCK_COUNT);
				hl1 = hl1->hl_next;
			}
		}
		else {
			// Deduplicate the segment and store the unique blocks
			// Update metadata maps
			for (j=0; j<count; j++)
				bpba[j] = bbnos[j] = 0;
			sn->md->bkt->searchHashesWrap(bn+HASH_BUCK_COUNT, hl1, count, bpba, bbnos, brefcounts);
			// First add missing hashes 
			for (j=0; j<count; j++) {
				if (bpba[j] == 0) {
					// Hash not found in the bucket
					pba[i+j] = sn->bmap->allocBlocks(1);
					assert(pba[i+j] > 0);
					sn->UNIQUE_BLOCK_COUNT ++;
				}
			}

			for (j=0; j<count; j++) {
				if (bpba[j] != 0) {
					sn->md->bkt->incrementRefValWrap(bbnos[j], bpba[j]);
					pba[i+j] = bpba[j];
				}
				else {
					sn->md->bkt->insertValWrap(bn+HASH_BUCK_COUNT, pba[i+j], hl1->hl_hash, 1);
					sn->md->pb->insertValWrap(pba[i+j], bn+HASH_BUCK_COUNT);
				}
				sn->md->bt->insertValWrap(clba+j, pba[i+j]);
				hl1 = hl1->hl_next;
			}
		}
		clba += count;
	}
}

void addNewFile(StorageNode *sn, unsigned char filehash[], unsigned int lba, 
		unsigned int nblocks, unsigned int pba[]) {
	unsigned int fbpba, fbnb, fbnext, fbent, fbentstart;
	unsigned int i, k, nb, stpba;

	// Not present, so add the entry
	k = 0;
	fbentstart = fbent = sn->md->fbl->getFreeFbe();
	fbpba = 0;
	fbnb = 0;
	fbnext = 0;

	while  (k<nblocks) {
		nb = (k+16 <= nblocks) ? 16 : (nblocks - k);
		stpba = sn->bmap->allocBlocks(nb);
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
			fbnext = sn->md->fbl->getFreeFbe();

			// Save old segment
			sn->md->fbl->fbePutEntry(fbent, fbpba, fbnb, fbnext);

			fbent = fbnext;
			fbpba = stpba;
			fbnb = nb;
			fbnext = 0;
		}

		for (i=0; i<nb; i++) {
			sn->md->bt->insertValWrap(lba+k+i, stpba+i);
			pba[k+i] = stpba+i;
		}
		k += nb;
	}

	// Save the last segment
	sn->md->fbl->fbePutEntry(fbent, fbpba, fbnb, fbnext);

	sn->UNIQUE_BLOCK_COUNT += nblocks;
	sn->md->fhbt->insertValWrap(filehash, fbentstart, nblocks, 1);
}

void addLbaMap(StorageNode *sn, unsigned int lba, unsigned int nb, 
		unsigned int stent) {
	unsigned int i, k;
	unsigned int fbpba, fbnb, fbnext, fbe;

	k = 0;
	fbe = stent;
	while (fbe != 0) {
		sn->md->fbl->fbeGetEntry(fbe, &fbpba, &fbnb, &fbnext);
		
		for (i=0; i<fbnb; i++) 
			sn->md->bt->insertValWrap(lba+k+i, fbpba+i);
		k += fbnb;
		fbe = fbnext;
	}
	if (k != nb) 
		cout << "Error: addLbaMap lba " << lba << " nb " << nb 
			<< " k " << k << endl;
}

// Large file deduplication, for L type
void processWholeFile(StorageNode *sn, struct FileBlocks *fb, 
	unsigned int pba[]) {
	static unsigned int maxsize = 0;
	static unsigned char *buf = NULL;
	unsigned int i, size;
	HashList *hl;
	unsigned char filehash[16];
	unsigned int stent, nblks, refs;
	//unsigned int fbpba, fbnb, fbnext, fbent, fbentstart;

	clock_add(sn, fb->fb_nb * 30);
	sn->t0_hash += (fb->fb_nb * 30);

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
	sn->md->fhbt->searchValWrap(filehash, &stent, &nblks, &refs);
	if (stent == 0) {
		addNewFile(sn, filehash, fb->fb_lbastart, fb->fb_nb, pba);
	}
	else {
		// Check if exact match is there?
		if (nblks == fb->fb_nb) {
			sn->md->fhbt->incrementRefCountWrap(filehash);
			addLbaMap(sn, fb->fb_lbastart, fb->fb_nb, stent);
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

void getOldMaps(StorageNode *sn, struct segment *s, unsigned int pba[],
unsigned int bnos[], unsigned int fbnos[], unsigned int refs[]) {
	unsigned int i;

	for (i=0; i<s->seg_size; i++) {
		sn->md->bt->searchValWrap(s->seg_start+i, &pba[i]);

		if (pba[i] != 0) {
			sn->md->pb->searchValWrap(pba[i], &bnos[i]);

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

void dedupSmallSegment(StorageNode *sn, struct segment *s, unsigned int pba[], 
		unsigned int bnos[], unsigned int opba[], unsigned int obno[], 
		unsigned int oref[]) {
	unsigned int pba1;
	unsigned int refcount;
	unsigned int hbno;
	int retval;
	CacheEntry *c1;
	
	// Find the hash bucket corresponding to the hash
	// Search for the exact matching hash
	sn->md->hbkt->searchValWrap(s->seg_hash[0], &pba1, &refcount);
			
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
			pba[0] = sn->bmap->allocBlocks(1);
			assert(pba[0] > 0);
			sn->UNIQUE_BLOCK_COUNT ++;
			sn->md->hbkt->insertValWrap(hbno, pba[0], s->seg_hash[0], 1);
			//dupcounts[0][0]++;
			sn->md->bt->insertValWrap(s->seg_start, pba[0]);
			sn->md->pb->insertValWrap(pba[0], hbno);
		}
		else {
			pba[0] = pba1;
			//dupcounts[0][1]++;
			// Increment the pba reference count and add lba entry
			sn->md->hbkt->incrementRefValWrap(hbno, pba1);
			sn->md->bt->insertValWrap(s->seg_start, pba1);
		}
	}
	else {
		// If pba1 and pba[0] are same
		// nothing needs to be done.
		if (pba1 != opba[0]) {
			if (obno[0] > HASH_BUCK_COUNT) 
				retval = sn->md->bkt->decrementRefValWrap(obno[0], opba[0]); 
			else
				retval = sn->md->hbkt->decrementRefValWrap(obno[0], opba[0]);
			if (retval == 0) {
				sn->md->pb->deleteValWrap(opba[0]);
				// Free the block, and make
				// the cache entry reusable
				sn->bmap->freeBlock(opba[0]);
				c1 = sn->dc->searchCache(opba[0]);
				if (c1 != NULL) {
					sn->dc->deleteEntry(c1);
					sn->mem->cefree(c1);
				}
				sn->UNIQUE_BLOCK_COUNT--;
			}

			if (pba1 == 0) {
				pba1 = sn->bmap->allocBlocks(1);
				assert(pba1 > 0);
				sn->UNIQUE_BLOCK_COUNT ++;
				sn->md->hbkt->insertValWrap(hbno, pba1, s->seg_hash[0], 1);
				sn->md->pb->insertValWrap(pba1, hbno);
				//dupcounts[0][0]++;
			}
			else {
				sn->md->hbkt->incrementRefValWrap(hbno, pba1);
				//dupcounts[0][1]++;
			}

			// Update lba2pba
			sn->md->bt->updateValWrap(s->seg_start, pba1);
			pba[0] = pba1;
		}
	}
	bnos[0] = hbno;
}

void dedupLargeSegment(StorageNode *sn, struct segment *s, unsigned int pba[], 
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
	bn = sn->md->bkt->findBucket(rid);

#ifdef	__DEBUG__
	cout << "large segment bkt " << bn << endl;
#endif
	if (bn == 0) {	
		// New bucket is to be created
		// Add this segment to new bucket
		bnonew = sn->md->bkt->addNewBucket(rid);
		assert(bnonew > 0);

		// Allocate new blocks 
		seg_pba = sn->bmap->allocBlocks(s->seg_size);
		assert(seg_pba > 0);
		sn->UNIQUE_BLOCK_COUNT += s->seg_size;

		// New bucket, so free all lba-pba-bno entries
		// Decrement pba reference counts 
		// for old pba values
		for (i=0; i<s->seg_size; i++) {
			if (opba[i] != 0) {
				sn->md->bt->deleteValWrap(s->seg_start+i);

				if (obno[i] <= HASH_BUCK_COUNT)
					retval = sn->md->hbkt->decrementRefValWrap(obno[i], opba[i]);
				else
					retval = sn->md->bkt->decrementRefValWrap(obno[i], opba[i]);
				if (retval == 0) {
					sn->md->pb->deleteValWrap(opba[i]);
					// Free the block, and make
					// the cache entry reusable
					sn->bmap->freeBlock(opba[i]);
					c1 = sn->dc->searchCache(opba[i]);
					if (c1 != NULL) {
						sn->dc->deleteEntry(c1);
						sn->mem->cefree(c1);
					}
					sn->UNIQUE_BLOCK_COUNT--;
				}
			}
		}

		// Add lba-pba-bno new entries
		for (i=0; i<s->seg_size; i++) {
			npba[i] = seg_pba + i;
			bnos[i] = bnonew + HASH_BUCK_COUNT;
			sn->md->bkt->insertValWrap(bnos[i], npba[i], s->seg_hash[i], 1);
			sn->md->bt->insertValWrap(s->seg_start+i, npba[i]);
			sn->md->pb->insertValWrap(npba[i], bnos[i]);
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

		sn->md->bkt->searchHashesWrap(bn+HASH_BUCK_COUNT, s, bpba, bbnos, brefcounts);
		// First add missing hashes 
		for (i=0; i<s->seg_size; i++) {
			if (bpba[i] == 0) {
				// Hash not found in the bucket
				npba[i] = sn->bmap->allocBlocks(1);
				assert(npba[i] > 0);
				sn->UNIQUE_BLOCK_COUNT ++;
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
					sn->md->bkt->incrementRefValWrap(bbnos[i], bpba[i]);

					if (opba[i] == 0)
						sn->md->bt->insertValWrap(s->seg_start+i, bpba[i]);
					else
						sn->md->bt->updateValWrap(s->seg_start+i, bpba[i]);
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
				sn->md->bkt->insertValWrap(bn+HASH_BUCK_COUNT, npba[i], s->seg_hash[i], 1);
				if (opba[i] == 0)
					sn->md->bt->insertValWrap(s->seg_start+i, npba[i]);
				else
					sn->md->bt->updateValWrap(s->seg_start+i, npba[i]);
				sn->md->pb->insertValWrap(npba[i], bn+HASH_BUCK_COUNT);
				pba[i] = npba[i];
			}
		}

		// Decrement the reference counts of the 
		// old remembered pba's
		for (i=0; i<dpbacount; i++) {
			if (dbno[i] <= HASH_BUCK_COUNT)
				retval = sn->md->hbkt->decrementRefValWrap(dbno[i], dpba[i]);
			else
				retval = sn->md->bkt->decrementRefValWrap(dbno[i], dpba[i]);
			if (retval == 0) {
				sn->md->pb->deleteValWrap(dpba[i]);
				// Free the block, and make
				// the cache entry reusable
				sn->bmap->freeBlock(dpba[i]);
				c1 = sn->dc->searchCache(dpba[i]);
				if (c1 != NULL) {
					sn->dc->deleteEntry(c1);
					sn->mem->cefree(c1);
				}
				sn->UNIQUE_BLOCK_COUNT--;
			}
		}
	}
	//cout << "dedupLargeSegment step 2" << endl;
	//cout << "dedupLargeSegment step 3" << endl;
}


void dedupSegment(StorageNode *sn, struct segment *s, unsigned pba[]) {
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
	// Move the lba blocks from lsn->dc to sn->dc, with appropriately
	// mapped pba values.

	
	// Add md5 hash calculation times
	clock_add(sn, s->seg_size*30);

	for (i=0; i<s->seg_size; i++)
		opba[i] = pba[i] = obno[i] = fbno[i] = bno[i] = oref[i] = 0;

	getOldMaps(sn, s, opba, obno, fbno, oref);	

	//cout <<	"dedupSegment 1" << endl;
	// For smaller segments (block 1):
	if (s->seg_size == 1) 
		dedupSmallSegment(sn, s, pba, bno, opba, obno, oref);
	else 
		dedupLargeSegment(sn, s, pba, bno, opba, obno, fbno, oref);
}

// Read request processing. Indeintify the corresponding pba's 
void processReadRequest(StorageNode *sn, struct segment *s, 
		unsigned int pba[]) {
	int i; 


	for (i=0; i<s->seg_size; i++) pba[i] = 0; 
	
	// First search in the present write buffers with the corresponding
	// LBA values. Assuming that the corresponding data is available in 
	// the lsn->dc cache, process those LBA reads directly from cache. The 
	// corresponding Unmapped pba entries are retained as zero's
	
	// If pba's are found for the corresponding lba's use the pba's and 
	// search sn->dc cache for the data blocks.

	// If there is no old LBA or corresponding pba, then this is read
	// corresponding to old write, which is not in the trace. So generate 
	// a corresponding write operation with old time stamp value.

		for (i=0; i<s->seg_size; i++) // Find new mappings
			sn->md->bt->searchValWrap(s->seg_start+i, &pba[i]);

	// For the missing blocks search in write request buffer, 
	// if necessary add write requests at the beginning
}

void processWriteRequest(StorageNode *sn, struct segment *s, 
		unsigned int pba[]) {

	dedupSegment(sn, s, pba);

	// First search in the present write buffers with the corresponding
	// LBA values. Overwrite the old request. 
	// The corresponding Unmapped pba entries are retained 
	// as zero's. For such writes add the data to the lsn->dc cache.
}


void readSegmentData(StorageNode *sn, struct segment *s, unsigned int pba[], unsigned long long int ts, unsigned long long int incr) {
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

		readSegmentDC(sn, pbastart, count, rtime, incr);
		rtime += (incr * count);
	}
}

void writeSegmentData(StorageNode *sn, struct segment *s, unsigned int pba[], unsigned long long int ts, unsigned long long int incr) {
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

		writeSegmentDC(sn, pbastart, count, wtime, incr);
		wtime += (incr * count);
	}
}


// Apply deduplication information and find the pba addresses of
// the read requests lba's
void dedupReadRequest(StorageNode *sn, struct segment *s, unsigned int pba[]) {
	int i; 
	unsigned int lba; 

	// If the operation is read try to find the physical 
	// block addresses. If not present it can be assumed 
	// that the blocks were written before and now being 
	// read. So for simulation purpose find any duplicate 
	// blocks exist with the same hash value. If not 
	// existing, then allocate the physical blocks.

	//dupcnt = s->seg_size;
	for (i=0; i<s->seg_size; i++) {
		lba = s->seg_start + i;
		sn->md->bt->searchValWrap(lba, &pba[i]);

		//if (pba[i] == 0) {
			//OPERATION = OPERATION_WRITE;
			// Not found in the lba index
			// deduplicate this lba read
			//sn->md->hbt->searchValWrap(s->seg_hash[i], &pba[i]);

			//if (pba[i] == 0) {
				// Not found in hash index also
				// Add an entry to hash index
				//pba[i] = allocBlocks(1); 
				//sn->UNIQUE_BLOCK_COUNT++;
				//assert(pba[i] > 0);
				//sn->md->hbt->insertValWrap(s->seg_hash[i], pba[i]);
			//}

			// Add an entry to lba index
			//sn->md->bt->insertValWrap(lba, pba[i]);

			// Increment the pba reference count
			//sn->md->pbaref->incrementPbaRef(pba[i], s->seg_hash[i]);
			//OPERATION = OPERATION_READ;
		//}
	}
}

void dedupWriteRequest(StorageNode *sn, struct segment *s, unsigned int pba[]) {
	int i; 
	unsigned int pba2, lba; 

	//dupcnt = s->seg_size;

	// If the operation is write try to find the duplicate 
	// physical block addresses within finger print cache. 
	// If not present search the hash index. If there also
	// not found then add new entry to hash index.
	// Search the lba index also. If exists and finger 
	// print are different, then update the finger print.
	// If not present then add a new entry.

	clock_add(sn, s->seg_size*30);

	for (i=0; i<s->seg_size; i++) {
		lba = s->seg_start + i;

		// Search for the duplicate hash in hash index
		sn->md->hbt->searchValWrap(s->seg_hash[i], &pba[i]);

		if (pba[i] == 0) {
			// New one, Allocate a block and add hash entry
			pba[i] = sn->bmap->allocBlocks(1); 
			sn->UNIQUE_BLOCK_COUNT++;
			assert(pba[i] > 0);
			sn->md->hbt->insertValWrap(s->seg_hash[i], pba[i]);
			//dupcnt--;
		}

		// Get the old pba for this lba if exists
		sn->md->bt->searchValWrap(lba, &pba2);

		if (pba2 == 0) {
			sn->md->bt->insertValWrap(lba, pba[i]);
			sn->md->pbaref->incrementPbaRef(pba[i], s->seg_hash[i]);
		}
		else if (pba[i] != pba2) {
			sn->md->pbaref->decrementPbaRef(pba2, sn->md);
			//m->bt->deleteValWrap(lba);
			sn->md->bt->updateValWrap(lba, pba[i]);
			sn->md->pbaref->incrementPbaRef(pba[i], s->seg_hash[i]);
		}
	}
	//countSegmentLength(pba, s->seg_size);
}

// Process read file 
void processReadFile(StorageNode *sn, struct FileBlocks *fb, 
	unsigned int pba[]) {
	unsigned int clba;
	unsigned int i;

	// Find all logical to physical block maps
	clba = fb->fb_lbastart;
	for (i=0; i<fb->fb_nb; i++) 
		sn->md->bt->searchValWrap(clba+i, &pba[i]);
}

void processSegmentReadNative(StorageNode *sn, struct segment4k *s, 
		unsigned long long int ts1, unsigned long long int ts2) {
	int i, j, m, k, scount;
	unsigned long long int incr, rtime, wtime;
	unsigned long int dtime, overhead;
	struct timespec tval;
	unsigned int pba1, pba2, seg_size;
	short int rwflag;
	CacheEntry *cent, *c;
	int ind;

	sn->DATA_BLOCK_READ_COUNT += (s->seg_size * 8);
	s->seg_start /= 8;
	if (s->seg_size > 1)
		incr = (ts2 - ts1) / (s->seg_size - 1);
	else	incr = 2;
	seg_size = s->seg_size;
	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);
		
	m = 0;
	// Split and process home requests which are very larger
	while (m < seg_size) {
		// Search for the data blocks segment by segment 
		// in the cache with the representative pba number
		i = 0;
		rtime = wtime = 0;
		rtime = ts1;
		for (j=0; j<s->seg_size; j++) {
			pba1 = s->seg_start + j;
			c = sn->dc->searchCache(pba1);
			if (c != NULL) {
				sn->DATA_BLOCK_READ_CACHE_HIT += 8;
				c->ce_rtime = rtime;
				c->ce_refcount++;
				sn->dc->c_rrefs++;
#ifdef  DARCP
				sn->dc->repositionEntry(c);
#endif
				rtime += incr; 
				sn->curTime += incr;
			}
			else {
				// Find the sequence of blocks not in 
				// data cache, and issue single request
				scount = 1;
				pba2 = pba1+1;
				j++;

				while ((j < s->seg_size) && ((c = sn->dc->searchCache(pba2)) == NULL)) {
					j++; scount++; pba2++;
				}
				j--; //Go back to the last block
						

				DiskRead(sn, sn->curTime, DATADISK, 8*(pba1), scount*8);

				rwflag = CACHE_ENTRY_CLEAN;
				sn->curTime += (incr * scount);
				// Add missing data block entries to the cache
				for (k=0; k<scount; k++) {
					pba2 = pba1 + k;

					sn->DATA_BLOCK_EFFECTIVE_WRITES += sn->dc->addReplaceEntry(dummy, pba2, NULL, 4096, rtime, wtime, DATADISK, rwflag, NULL, &cent);
				}
				rtime += (incr * scount); 
			}
		}
		ts1 += (incr * s->seg_size);
		m += s->seg_size;
	}

	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime*1000 + overhead;	// Microseconds
	update_statistics(sn->r_var, sn->r_mean, sn->r_min, sn->r_max, sn->r_n, seg_size*8, ind);
}

void processSegmentWriteNative(StorageNode *sn, struct segment4k *s, 
		unsigned long long int ts1, unsigned long long int ts2) {
	int i, j, m, k, scount;
	unsigned long long int incr, rtime, wtime;
	unsigned long int dtime, overhead;
	struct timespec tval;
	unsigned int pba1, pba2, seg_size;
	short int rwflag;
	CacheEntry *cent, *c;
	unsigned int ind;

	sn->DATA_BLOCK_WRITE_COUNT += (s->seg_size * 8);

	s->seg_start /= 8;
	if (s->seg_size > 1)
		incr = (ts2 - ts1) / (s->seg_size - 1);
	else	incr = 2;
	seg_size = s->seg_size;
	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);
	rwflag = CACHE_ENTRY_DIRTY;	
	// Split and process home requests which are very larger
	// Search for the data blocks segment by segment 
	// in the cache with the representative pba number
	i = 0;
	rtime = 0;
	wtime = ts1;
	for (j=0; j<s->seg_size; j++) {
		pba1 = s->seg_start + j;
		c = sn->dc->searchCache(pba1);
		if (c != NULL) {
			sn->DATA_BLOCK_WRITE_CACHE_HIT += 8;
			c->ce_wtime = wtime;
			c->ce_refcount++;
			c->ce_wrefcount++;
			sn->dc->c_wrefs++;
			if ((c->ce_flags & CACHE_ENTRY_DIRTY) == 0) {
				c->ce_flags |= CACHE_ENTRY_DIRTY;
				sn->dc->c_dcount++;
			}
#ifdef  DARCP
			sn->dc->repositionEntry(c);
#endif
			wtime += incr;
			sn->curTime += incr;
		}
		else {
			// Find the sequence of blocks not in 
			// data cache, and issue single request
			scount = 1;
			pba2 = pba1+1;
			j++;

			while ((j < s->seg_size) && ((c = sn->dc->searchCache(pba2)) == NULL)) {
				j++; scount++; pba2++;
			}
			j--; //Go back to the last block
					
			// Add missing data block entries to the cache
			for (k=0; k<scount; k++) {
				pba2 = pba1 + k;

				sn->DATA_BLOCK_EFFECTIVE_WRITES += sn->dc->addReplaceEntry(dummy, pba2, NULL, 4096, rtime, wtime, DATADISK, rwflag, NULL, &cent);
			}
			wtime += (incr * scount);
		}
	}

	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime*1000 + overhead; // Microseconds
	update_statistics(sn->w_var, sn->w_mean, sn->w_min, sn->w_max, sn->w_n, seg_size*8, ind);
}

void processSegmentReadHDS(StorageNode *sn, struct segment4k *s1, 
		unsigned long long int ts1, unsigned long long int ts2) {
	int i, j; 
	int op; 
	unsigned int pba[MAX_SEG_SIZE];
	unsigned long long int incr;
	struct segment s;
	struct segmenthome s2;
	int l;
	int flag, len;
	int retval; //, trace15 = 0;
	unsigned int ind;
	unsigned long dtime, overhead;
	int seg_size;
	IOTraceReader *trace;

	sn->DATA_BLOCK_READ_COUNT += (s1->seg_size * 8);

	s1->seg_start /= 8;
	if (s1->seg_size > 1)
		incr = (ts2 - ts1) / (s1->seg_size - 1);
	else	incr = 2;
	seg_size = s1->seg_size;

	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);

	l = 0;
	// Split and process home requests which are very larger
	while (l < seg_size) {
		s.seg_start = s1->seg_start + l;
		memcpy(s.seg_hash[0], s1->seg_hash[l], 16);
		len = 1;
		for (i=1; (i<MAX_SEG_SIZE_USED) && ((l + i) < s1->seg_size); i++) {
			flag = 1;
			for (j=0; j < i; j++) {
				if (memcmp(s.seg_hash[j], s1->seg_hash[l+i], 16) == 0) {
					flag = 0;
					break;
				}
			}
			if (flag == 0) break;
				
			memcpy(s.seg_hash[i], s1->seg_hash[l+i], 16);
				len++;
		}
		s.seg_size = len;

#ifdef	__DEBUG	
		cout <<  "\n\t\tseg : " << s.seg_start << " , " << 8*s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

		processReadRequest(sn, &s, pba);
		
		// Search for the data blocks segment by segment in the cache
		// with the representative pba number
		readSegmentData(sn, &s, pba, ts1, incr);
		ts1 += (incr * s.seg_size);
		l += s.seg_size;
	}
	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime * 1000 + overhead;
	update_statistics(sn->r_var, sn->r_mean, sn->r_min, sn->r_max, sn->r_n, seg_size*8, ind);
}

void processSegmentWriteHDS(StorageNode *sn, struct segment4k *s1, 
		unsigned long long int ts1, unsigned long long int ts2) {
	int next_pid = -1;
	int i, j; 
	int op; 
	unsigned int pba[MAX_SEG_SIZE];
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int incr;
	struct segment s;
	//struct segment4k s1;
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

	sn->DATA_BLOCK_WRITE_COUNT += (s1->seg_size * 8);
	s1->seg_start /= 8;
	if (s1->seg_size > 1)
		incr = (ts2 - ts1) / (s1->seg_size - 1);
	else	incr = 2;
	seg_size = s1->seg_size;

	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);

	l = 0;
	// Split and process home requests which are very larger
	while (l < seg_size) {
		s.seg_start = s1->seg_start + l;
		memcpy(s.seg_hash[0], s1->seg_hash[l], 16);
		len = 1;
		for (i=1; (i<MAX_SEG_SIZE_USED) && ((l + i) < s1->seg_size); i++) {
			flag = 1;
			for (j=0; j < i; j++) {
				if (memcmp(s.seg_hash[j], s1->seg_hash[l+i], 16) == 0) {
					flag = 0;
					break;
				}
			}
			if (flag == 0) break;
			
			memcpy(s.seg_hash[i], s1->seg_hash[l+i], 16);
			len++;
		}
		s.seg_size = len;

#ifdef	__DEBUG	
		cout <<  "\n\t\tseg : " << s.seg_start << " , " << 8*s.seg_start << ", " << s.seg_size << ", " << seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

		processWriteRequest(sn, &s, pba);
	
		//cout << "step 1" << endl;	
		// Search for the data blocks segment by segment in the cache
		// with the representative pba number
		writeSegmentData(sn, &s, pba, ts1, incr);
		//cout << "step 2" << endl;	

		ts1 += (incr * s.seg_size);
		l += s.seg_size;
		countSegmentLength(pba, s.seg_size);
	}

	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime * 1000 + overhead;
	update_statistics(sn->w_var, sn->w_mean, sn->w_min, sn->w_max, sn->w_n, seg_size*8, ind);
}

void processSegmentReadFull(StorageNode *sn, struct segment4k *s1, 
		unsigned long long int ts1, unsigned long long int ts2) {
	int next_pid = -1;
	int i, j, k, m; 
	int count = 0, scount, op; 
	unsigned int pba1, pba2, pba[MAX_SEG_SIZE], pbastart;
	CacheEntry *c; 
	unsigned long long int rtime, wtime;
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int incr;
	struct segment s;
	struct segmenthome s2;
	//struct segment4k s1;
	//unsigned int twritecount;
	unsigned short rwflag;
	int retval; //, trace15 = 0;
	unsigned long int ind;
	unsigned long int overhead, dtime;
        //int tno;
	int seg_size;
	//struct timespec tval;

	sn->DATA_BLOCK_READ_COUNT += (s1->seg_size * 8);
		
	s1->seg_start /= 8;
	if (s1->seg_size > 1)
		incr = (ts2 - ts1) / (s1->seg_size - 1);
	else 	incr = 2;
	seg_size = s1->seg_size;

	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);

#ifdef	__DEBUG__
	cout <<  "\t\tseg : " << s.seg_start << " , " << s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

	dedupReadRequest(sn, (struct segment *)s1, pba);
	readSegmentData(sn, (struct segment *)s1, pba, ts1, incr);
		
	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime * 1000 + overhead;
	update_statistics(sn->r_var, sn->r_mean, sn->r_min, sn->r_max, sn->r_n, seg_size*8, ind);
}

void processSegmentWriteFull(StorageNode *sn, struct segment4k *s1, 
		unsigned long long int ts1, unsigned long long int ts2) {
	int next_pid = -1;

	int i, j, k, m; 
	int count = 0, scount, op; 
	unsigned int pba1, pba2, pba[MAX_SEG_SIZE], pbastart;
	CacheEntry *c; 
	unsigned long long int rtime, wtime;
	//unsigned int lastFlushTime = 0; //, lastGCTime = 0;
	unsigned long long int incr;
	struct segment s;
	struct segmenthome s2;
	unsigned int twritecount;
	unsigned short rwflag;
	int retval; //, trace15 = 0;
	unsigned long int ind;
	unsigned long int overhead, dtime;
        int tno = 0;
	int seg_size;
	//MDMaps *m;
	struct timespec tval;


	sn->DATA_BLOCK_WRITE_COUNT += (s1->seg_size * 8);
	s1->seg_start /= 8;
	if (s1->seg_size > 1)
		incr = (ts2 - ts1) / (s1->seg_size - 1);
	else 	incr = 2;
	seg_size = s1->seg_size;
	sn->t0_hash += (seg_size * 30);

	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);

	m = 0;

	// Split and process home requests which are very larger
	while (m < seg_size) {
		s.seg_start = s1->seg_start + m;
		for (i=0; (i<MAX_SEG_SIZE) && ((m + i) < s1->seg_size); i++)
			memcpy(s.seg_hash[i], s1->seg_hash[m+i], 16);
		s.seg_size = i;
#ifdef	__DEBUG__
		cout <<  "\t\tseg : " << s.seg_start << " , " << s.seg_start << ", " << s.seg_size << ((op == OP_READ) ? " Read " : " Write ") << trace[tno]->getCurCount() << endl;
#endif

		dedupWriteRequest(sn, &s, pba);
		writeSegmentData(sn, &s, pba, ts1, incr);
		
		ts1 += (incr * s.seg_size);
		m += s.seg_size;
		countSegmentLength(pba, s.seg_size);
	}

	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime * 1000 + overhead;
	update_statistics(sn->w_var, sn->w_mean, sn->w_min, sn->w_max, sn->w_n, seg_size*8, ind);
}

void processFileReadApp(StorageNode *sn, struct FileBlocks *fb, 
		unsigned int pba[],
		unsigned long long int ts1, unsigned long long int ts2) {
	unsigned long long int incr;
	int retval; 
	unsigned int ind;
	unsigned long dtime, overhead;

	sn->DATA_BLOCK_READ_COUNT += fb->fb_nb;
	fb->fb_nb /= 8;
	fb->fb_lbastart /= 8;
		
	if (fb->fb_nb > 1)
		incr = (ts2 - ts1) / (fb->fb_nb - 1);
	else	incr = 2;

	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);

	processReadFile(sn, fb, pba);

	readData(sn, pba, fb->fb_nb, ts1, incr);

	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime * 1000 + overhead;
	update_statistics(sn->r_var, sn->r_mean, sn->r_min, sn->r_max, sn->r_n, fb->fb_nb*8, ind);
}

void processFileWriteApp(StorageNode *sn, struct FileBlocks *fb, 
		int ftype, unsigned int pba[],
		unsigned long long int ts1, unsigned long long int ts2) {
	int i; 
	int op; 
	unsigned long long int incr;
	unsigned int twritecount;
	int retval; 
	unsigned int ind;
	unsigned long dtime, overhead;
	struct timespec tval;

	sn->DATA_BLOCK_WRITE_COUNT += fb->fb_nb;
	fb->fb_nb /= 8;
	fb->fb_lbastart /= 8;
	if (fb->fb_nb > 1)
		incr = (ts2 - ts1) / (fb->fb_nb - 1);
	else	incr = 2;

	sn->curTime = ts1;
	if (incr <= 0) incr = 2;

	clock_start(sn);
	disk_start(sn);

	if (fb->fb_nb <= SMALL_FILE_BLKS) {
			// Small file
		processSmallFile(sn, fb, pba);
	}
	else {
		// Large file
		if (ftype == LTYPE)
			processWholeFile(sn, fb, pba);
		else	processHULargeFile(sn, fb, pba);
	}
	writeData(sn, pba, fb->fb_nb, ts1, incr);
	countSegmentLength(pba, fb->fb_nb);

	overhead = clock_stop(sn);
	dtime = disk_stop(sn);

	ind = dtime * 1000 + overhead;
	update_statistics(sn->w_var, sn->w_mean, sn->w_min, sn->w_max, sn->w_n, fb->fb_nb*8, ind);
}


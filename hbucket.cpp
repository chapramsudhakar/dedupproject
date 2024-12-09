#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "hbucket.h"
#include "ibtree.h"
#include "util.h"
#include "cachemem.h"
#include "cachesize.h"

extern unsigned long long int curTime;
extern unsigned long long int UNIQUE_BLOCK_COUNT;

extern int OPERATION;
extern IBTree pb;
extern HashBucket *hbkt;

HashBucket::HashBucket(const char *hbktfname, StorageNode *sn) {
	unsigned int i;
	//string fname = "hashbuckets.dat";
	hashbucket b;
	b_sn = sn;

	// Open the buckets database file
	bfs.open(hbktfname, ios::in | ios:: out | ios::binary);
	if (bfs.fail()) {
		cout << "HashBuckets file opening failure : " << hbktfname << endl;
		exit(0);
	}
	bfs.clear();

	// Initailize the bucket index and bucket cache
#ifdef	DARCP
	bc = new DARC(hbktfname, HBUCKET_CACHE_LIST, HBUCKET_CACHE_SIZE, 512, b_sn);
#else
	bc = new LRU(hbktfname, HBUCKET_CACHE_LIST, HBUCKET_CACHE_SIZE, b_sn);
#endif

	maxbuckets = MAX_HBUCKETS;
	incr = 64;
	HBUCKET_READ_COUNT = 0;
	HBUCKET_READ_CACHEHIT = 0;
	HBUCKET_WRITE_COUNT = 0;
	HBUCKET_WRITE_CACHEHIT = 0;
	HBUCKET_EFFECTIVE_WRITES = 0;
	HBUCKET_FLUSH_WRITES = 0;
	HBUCKET_USED_COUNT = 64*1024;

	HBUCKET_READ_COUNT_READ = 0;
	HBUCKET_WRITE_COUNT_READ = 0;
	HBUCKET_READ_COUNT_WRITE = 0;
	HBUCKET_WRITE_COUNT_WRITE = 0;
	BKTSEARCH = 0;
	BKTINSERT = 0;
	BKTUPDATE = 0;
	BKTDELETE = 0;
	
	nextFreeHBucket = 64*1024 + 1;

	flist = new FreeList("hashbkt",256);
	// Find the current maxbuckets count
	bfs.seekg(0, bfs.end);
	maxbuckets = bfs.tellg() / sizeof(struct hashbucket);

	// Count the used blocks and find some free blocks
	for (i=64*1024+1; i<= maxbuckets; i++) {
		bfs.seekg((unsigned long)((i-1) * sizeof(struct hashbucket)));
		bfs.read((char *)&b, sizeof(struct hashbucket));

		if (b.hb_nent != 0) HBUCKET_USED_COUNT++;
		else flist->freeVal(b.hb_num);
	}

	//cout << hbktfname << ": max buckets : " << maxbuckets << " used " << HBUCKET_USED_COUNT << endl;
}

// Initialize key value cache
void HashBucket::enableKeyValueCache(unsigned int plsize, unsigned int bcount) {
	unsigned int i, n;
	struct BKTVal *bv1, *bv2;
	struct pbaList *pl1, *pl2;

	KVCacheSize = bcount * sizeof(struct BKTVal);
	KVHASH = bcount/20 + 1;

	//cout << "plsize " << plsize << " bcount " << bcount << " KVHASH " << KVHASH << endl;
	bv2 = (struct BKTVal *)malloc(bcount * sizeof(struct BKTVal));
	assert(bv2 != NULL);
	FL = bv1 = bv2;
	for (i=1; i<bcount; i++) {
		bv1->next = &bv2[i];
		bv1 = bv1->next;
	}
	bv1->next = NULL;

	KVQ = (struct BKTVal **)malloc(KVHASH * sizeof (struct BKTVal *));
	assert(KVQ != NULL);
	for (i=0; i<KVHASH; i++) KVQ[i] = NULL;
	KVTail = KVHead = NULL;

	plCacheSize = plsize;
	n = plsize / sizeof(struct pbaList);
	pl2 = (struct pbaList *)malloc(n * sizeof(struct pbaList));
	assert(pl2 != NULL);
	PFL = pl1 = pl2;

	for (i=1; i<n; i++) {
		pl1->next = &pl2[i];
		pl1 = pl1->next;
	}
	pl1->next = NULL;
	//cout << "FL " << FL << " PFL " << PFL << endl;
}


void HashBucket::displayStatistics(void) {
	cout << "HashBucket structure stats:\n";
	cout << "HashBucket Cache\n";
	bc->displayStatistics();
	cout << "HBUCKET_READ_COUNT \t\t= " << HBUCKET_READ_COUNT << endl
		<< "HBUCKET_READ_CACHEHIT \t\t= " << HBUCKET_READ_CACHEHIT << endl
		<< "HBUCKET_WRITE_COUNT \t\t= " << HBUCKET_WRITE_COUNT << endl
		<< "HBUCKET_WRITE_CACHEHIT \t\t= " << HBUCKET_WRITE_CACHEHIT << endl
		<< "HBUCKET_EFFECTIVE_READS \t= " << (HBUCKET_READ_COUNT - HBUCKET_READ_CACHEHIT)  << endl
		<< "HBUCKET_EFFECTIVE_WRITES \t= " << HBUCKET_EFFECTIVE_WRITES << endl
		<< "HBUCKET_FLUSH_WRITES \t= " << HBUCKET_FLUSH_WRITES << endl
                << "HBucket read operation read count \t=" << HBUCKET_READ_COUNT_READ << endl
                << "HBucket read operation write count \t=" << HBUCKET_WRITE_COUNT_READ << endl
                << "HBucket write operation read count \t=" << HBUCKET_READ_COUNT_WRITE << endl
                << "HBucket write operation write count \t=" << HBUCKET_WRITE_COUNT_WRITE << endl
		<< "HBKTINSERT \t\t\t=" << BKTINSERT << endl
		<< "HBKTSEARCH \t\t\t=" << BKTSEARCH << endl
		<< "HBKTUPDATE \t\t\t=" << BKTUPDATE << endl
		<< "HBKTDELETE \t\t\t=" << BKTDELETE << endl
		<< "HBUCKET_USED_COUNT \t\t= " << HBUCKET_USED_COUNT << endl
		<< "Maximum HashBuckets \t\t= " << maxbuckets << endl;
}


struct hashbucket * HashBucket::getBucket(unsigned int n) {
	CacheEntry * ce;
	struct hashbucket *b1;
	unsigned int wrval;

	assert(n > 0);


	// Find the bucket in the cache
	ce = bc->searchCache(n);
	if (ce != NULL)  {  // Entry found in the cache
		HBUCKET_READ_COUNT++;
		HBUCKET_READ_CACHEHIT++;
		ce->ce_rtime = curTime;
		ce->ce_refcount++;
		bc->c_rrefs++;
		bc->repositionEntry(ce);
		b1 = (struct hashbucket *)ce->ce_object;
		//cout << "get Hash bucket 1 : n " << n << " b_num " << b1->hb_num << endl;
	}
	else {
		// Read block and add to the cache
	
		b1 = (struct hashbucket *) b_sn->mem->b512malloc();

		assert(b1 != NULL);
		assert(readHBucket(n, b1) == 0);

		wrval = bc->addReplaceEntry(bfs, n, (void *)b1, 
			sizeof(struct hashbucket), curTime, 0, METADATADISK, 
			CACHE_ENTRY_CLEAN, NULL, &ce);

		HBUCKET_EFFECTIVE_WRITES += wrval;
		if (OPERATION == OPERATION_READ)
			HBUCKET_WRITE_COUNT_READ += wrval;
		else
			HBUCKET_WRITE_COUNT_WRITE += wrval;
	}
	//assert(n == b1->hb_num);
	lockEntry(ce);

	return b1;
}

void HashBucket::releaseBucket(unsigned int n, unsigned short dirty) {
	CacheEntry *c;

	c = bc->searchCache(n);
	if (dirty != 0) {
		if (!(c->ce_flags & CACHE_ENTRY_DIRTY))
			bc->c_dcount++;
		c->ce_flags |= dirty;
		HBUCKET_WRITE_COUNT++;
		HBUCKET_WRITE_CACHEHIT++;
	}
	unlockEntry(c);
}

int HashBucket::putBucket(unsigned int n, struct hashbucket *b) {
	CacheEntry * ce;
	struct hashbucket *b1;
	unsigned int wrval;

	assert(n > 0);

	//assert(n == b->hb_num);
	HBUCKET_WRITE_COUNT++;

	// Find the bucket in the cache and copy into that
	ce = bc->searchCache(n);
	if (ce != NULL)  {  // Entry found in the cache
		lockEntry(ce);
		HBUCKET_WRITE_CACHEHIT++;
		if (ce->ce_object != b)
			memcpy(ce->ce_object, (char *)b, sizeof(struct hashbucket));

		ce->ce_wtime = curTime;
		ce->ce_refcount++;
		if (!(ce->ce_flags & CACHE_ENTRY_DIRTY)) {
			ce->ce_flags |= CACHE_ENTRY_DIRTY;
			bc->c_dcount++;
		}
		ce->ce_wrefcount++;
		bc->c_wrefs++;
		bc->repositionEntry(ce);
		unlockEntry(ce);
	}
	else {
		// Add to the cache
		b1 = (struct hashbucket *) b_sn->mem->b512malloc();
		assert(b1 != NULL);
		memcpy(b1, b, sizeof(struct hashbucket));	

		wrval = bc->addReplaceEntry(bfs, n, (void *)b1, sizeof(struct hashbucket), 0, curTime, METADATADISK, CACHE_ENTRY_DIRTY, NULL, &ce);

		HBUCKET_EFFECTIVE_WRITES += wrval;
		if (OPERATION == OPERATION_READ)
			HBUCKET_WRITE_COUNT_READ += wrval;
		else
			HBUCKET_WRITE_COUNT_WRITE += wrval;
	}

	return 0;
}


// Read bucket number n
int HashBucket::readHBucket(unsigned int n, struct hashbucket *b) {

	assert(n > 0);

	HBUCKET_READ_COUNT++;

	if (OPERATION == OPERATION_READ)
		HBUCKET_READ_COUNT_READ++;
	else
		HBUCKET_READ_COUNT_WRITE++;

	// Seek to the required position in the bucket file
	// and read the structure.
	bfs.seekg((unsigned long)(n-1)*sizeof(struct hashbucket));
	if (bfs.fail()) {
		cout << "Error : readBucket seek failed\n";
		bfs.clear();
		return -1;
	}

	bfs.read((char *)b, sizeof(struct hashbucket));
	//assert(n == b->hb_num);
	DiskRead(b_sn, curTime, METADATADISK, n, 1);
	if (bfs.fail()) {
		cout << n << " Error : readBucket read failed\n";
		bfs.clear();
		return -1;
	}

	return 0;
}
/*
int HashBucket::evictor(fstream &fs, CacheEntry *ent, int disk) {
	struct hashbucket *b;

	b = (struct hashbucket *)ent->ce_object;
	//cout << "Evicting key " << ent->ce_key << " b_num " << b->hb_num << endl;
	//assert(ent->ce_key == b->hb_num);
	hbkt->writeHBucket(ent->ce_key, b);
	return 0;
}
*/

// Write bucket number n
int HashBucket::writeHBucket(unsigned int n, struct hashbucket *b) {

	assert(n > 0);

	HBUCKET_WRITE_COUNT++;

	if (OPERATION == OPERATION_READ)
		HBUCKET_WRITE_COUNT_READ++;
	else
		HBUCKET_WRITE_COUNT_WRITE++;

	// Seek to the required position in the bucket file
	// and write the structure.
	bfs.seekp((unsigned long)(n-1)*sizeof(struct hashbucket));
	if (bfs.fail()) {
		cout << "Error : readBucket seek failed\n";
		bfs.clear();
		return -1;
	}

	bfs.write((char *)b, sizeof(struct hashbucket));
	DiskWrite(b_sn, curTime, METADATADISK, n, 1);
	//assert(n == b->hb_num);
	if (bfs.fail()) {
		cout << n << " Error : readBucket read failed\n";
		bfs.clear();
		return -1;
	}

	return 0;
}

// Flush the bucket cache and bucket index cache
void HashBucket::flushCache(void) {
	unsigned int wrcount;

	wrcount = bc->flushCache(bfs, sizeof(struct hashbucket), METADATADISK);
	HBUCKET_EFFECTIVE_WRITES += wrcount;
	HBUCKET_FLUSH_WRITES += wrcount;
}

unsigned int HashBucket::isPbaListEmpty(unsigned int bno) {
	BKTVal *c;

	// First search Bucket cache
	c = bvsearch(bno);
	if (c != NULL) return 0;
	else return 1;
}

// Find a free bucket
unsigned int HashBucket::findFreeHBucket() { 
	struct hashbucket *b, b1;
	unsigned int i;
	unsigned int lastBucket; 
	unsigned int olastBucket;
	CacheEntry *c;

	lastBucket = nextFreeHBucket;
	if (lastBucket > maxbuckets) lastBucket = 64*1024+1;
	olastBucket = lastBucket;
	HBUCKET_USED_COUNT++;


	if ((i = flist->getVal()) > 0) {
		return i;
	}

	for (i=lastBucket; i<= maxbuckets; i++) {
		c = bc->lookCache(i);
		if (c != NULL) b = (hashbucket *)c->ce_object;
		else {
			assert(readHBucket(i, &b1) == 0);
			b = &b1;
		}

		if (b->hb_nent == 0) {
			nextFreeHBucket = i+1;
			return i;
		}
	}

	if ((HBUCKET_USED_COUNT + 40) < maxbuckets) {
		for (i=64*1024+1; i<olastBucket; i++) {
			c = bc->lookCache(i);
			if (c != NULL) b = (hashbucket *)c->ce_object;
			else {
				assert(readHBucket(i, &b1) == 0);
				b = &b1;
			}
			if (b->hb_nent == 0) {
				nextFreeHBucket = i+1;
				return i;
			}
		}
	}

	b = &b1;
	// No free hash bucket found, so expand the hash bucket list
	bfs.seekp(0, bfs.end);
	memset(b, 0, sizeof(struct hashbucket));
	for (i=1; i<=incr; i++) {
		b->hb_num = maxbuckets + i;
		bfs.write((char *)b, sizeof(struct hashbucket));
	}

	i = maxbuckets + 1;
	nextFreeHBucket = i+1;
	maxbuckets += incr;
	return i;
}

// Delete one block from bucket b
unsigned int HashBucket::deleteBlockBKT(struct hashbucket *b, unsigned int pba) {
        unsigned int j, k;
        unsigned int done = 0;

        // Search for the pba
        for (j=0; j<b->hb_nent; j++) {
                if (b->hb_pba[j] == pba) {
                        // Found, shift next entries
                        k = j+1;
                        while (k < b->hb_nent) {
                                b->hb_pba[k-1] = b->hb_pba[k];
                                memcpy(b->hb_fp[k-1], b->hb_fp[k], 16);
                                b->hb_pbaref[k-1] = b->hb_pbaref[k];
                                k++;
                        }
                        b->hb_nent--;
			putBucket(b->hb_num, b);
                        done = 1;
			BKTDELETE++;
                        break;
                }
        }

        return done;
}

// Delete bucket b from chain of buckets 
unsigned int HashBucket::deleteBKT(struct hashbucket *b) {
        struct hashbucket *b1;
        unsigned int bno1, bno2;

        if ((b->hb_nent == 0)  && (isPbaListEmpty(b->hb_num) == 1)) {
                flist->freeVal(b->hb_num);
                // Bucket is empty, so it should be 
                // deleted from the list of buckets
                HBUCKET_USED_COUNT--;
                bno1 = b->hb_prevbucket;
                bno2 = b->hb_nextbucket;

                // Update linked list   
                if (bno1 != 0) {
                        b1 = getBucket(bno1);
                        b1->hb_nextbucket = bno2;
                        releaseBucket(bno1, CACHE_ENTRY_DIRTY);
                }

                if (bno2 != 0) {
                        b1 = getBucket(bno2);
                        b1->hb_prevbucket = bno1;
                        releaseBucket(bno2, CACHE_ENTRY_DIRTY);
                }
        }
	return 0;
}

// Get first bucket in the chain of buckets
unsigned int HashBucket::getFirstBucket(struct hashbucket *b) {
        unsigned int bno, retval;

	if (b->hb_num <= 64 * 1024) return b->hb_num;

        retval = b->hb_num;
        bno = b->hb_prevbucket;
        while (bno != 0) {
                b = getBucket(bno);
                retval = bno;
                bno = b->hb_prevbucket;
                releaseBucket(retval, 0);
        }

        return retval;
}

// Decrement the reference count
int HashBucket::deletePba(unsigned int bno, unsigned int pba) {
	struct hashbucket *b2; 
	unsigned int bno2; 
	int done = 0;

	b2 = getBucket(bno);
	if (deleteBlockBKT(b2, pba) == 1) {
		if ((b2->hb_nent == 0) && (bno > 64 * 1024)) 
			deleteBKT(b2);
		releaseBucket(bno, CACHE_ENTRY_DIRTY);
		return 1;
	}

	bno2 = getFirstBucket(b2);
	releaseBucket(bno, 0);
		
	while ((bno2 != 0) && (!done)) {
		if (bno2 == bno) // Current bucket
			bno2 = b2->hb_nextbucket;
		else {
			// get the bucket
			b2 = getBucket(bno2);

			done = deleteBlockBKT(b2, pba);
			if (done && (b2->hb_nent == 0) &&
					(bno2 > 64 * 1024)) 
				deleteBKT(b2);

			bno2 = b2->hb_nextbucket;
			releaseBucket(b2->hb_num, 0);
		}
	}

	return done;
}

// Add one block 
unsigned int HashBucket::addBlockBKT(struct hashbucket *b, 
		unsigned char *hash, unsigned int pba, 
		unsigned int refcount) {
	int done = 0;

	// If the bucket has space for the block?
	if ((b->hb_nent + 1) <= BLKS_PER_HBKT) {
		b->hb_pba[b->hb_nent] = pba;
		b->hb_pbaref[b->hb_nent] = refcount;
		memcpy(b->hb_fp[b->hb_nent], hash, 16);
		b->hb_nent++;
		putBucket(b->hb_num, b);
		done = 1;
	}

	return done;
}

// Search for one hash in bucket b
unsigned int HashBucket::searchForHashBKT(struct hashbucket *b, 
		unsigned char hash[], unsigned int *pba, 
		unsigned int *refcount) {
	unsigned int j;
	unsigned int done = 0;

	// update in the current bucket b
	for (j=0; j<b->hb_nent; j++) {
		// Search for the hash
		if (memcmp(b->hb_fp[j], hash, 16) == 0) {
			*pba = b->hb_pba[j];
			*refcount = b->hb_pbaref[j];
			done = 1;
			break;
		}
	}

	return done;
}

unsigned int HashBucket::searchForHash(struct hashbucket *b, 
		unsigned char * hash, unsigned int *pba, 
		unsigned int *vbno, unsigned int *refcount) {
	struct hashbucket *b2;
	unsigned int done = 0;
	unsigned int bno, bno2, fb; 

	*vbno = 0;

	// Search in the current bucket
	if (searchForHashBKT(b, hash, pba, refcount) == 1) {
		if (b->hb_num <= HASH_BUCK_COUNT)
			*vbno = b->hb_num;
		else 	*vbno = getFirstBucket(b);
		return 1;
	}

	// If not found, search the entire chain of buckets
	fb = bno2 = getFirstBucket(b);
	bno = b->hb_num;

	while ((bno2 != 0) && (!done)){
		if (bno2 == bno) // current bucket?
			bno2 = b->hb_nextbucket;
		else {
			b2 = getBucket(bno2);

			done = searchForHashBKT(b2, hash, pba, refcount);
			if (done) *vbno = fb;

			bno2 = b2->hb_nextbucket;
			releaseBucket(b2->hb_num, 0);
		}
	}

	return done;
}

// Update one block in bucket b
unsigned int HashBucket::updateBlockBKT(struct hashbucket *b, 
		unsigned char *hash, unsigned int pba, 
		unsigned int refcount) {
	unsigned int j;
	unsigned int done = 0;

	// update in the current bucket b
	for (j=0; j<b->hb_nent; j++) {
		// Search for the pba
		if (b->hb_pba[j] == pba) {
			memcpy(b->hb_fp[j], hash, 16);
			b->hb_pbaref[j] = refcount;
			putBucket(b->hb_num, b);
			done = 1;
			break;
		}
	}

	return done;
}
/*
unsigned int HashBucket::updateBlock(struct hashbucket *b1, 
		unsigned char * hash, unsigned int pba, 
		unsigned int refcount) {
	struct hashbucket *b;
	unsigned int bno;
	int done = 0;
	unsigned int retval = 0;

	if (updateBlockBKT(b1, hash, pba, refcount) == 1) 
		return b1->hb_num;

	bno = getFirstBucket(b1);

	while ((!done) && (bno != 0)) {
		if (bno == b1->hb_num) 
			bno = b1->hb_nextbucket;
		else {
			b = getBucket(bno);
			done = updateBlockBKT(b, hash, pba, refcount);
			if (done) retval = b->hb_num;
			bno = b->hb_nextbucket;
			releaseBucket(b->hb_num, 0);
		}
	}

	return retval;
}
*/
unsigned int HashBucket::updateBlocksBKT(struct hashbucket *b1, 
		struct pbaList ** l, struct pbaList **comp) {
	int modified = 0;
	pbaList *tl1, *tl2, *compTail;

	// Go to the tail end of comp list
	compTail = NULL;
	tl1 = *comp;
	while (tl1 != NULL) {
		compTail = tl1;
		tl1 = tl1->next;
	}

	// Update the entries within the bucket b1
	tl1 = *l;
	tl2 = NULL;  // Predcessor

	while (tl1 != NULL) {
		if (updateBlockBKT(b1, tl1->fp, tl1->pba, tl1->refcount) == 1) {
			// Updated, so remove it and add to completed list
			if (tl2 == NULL) {
				*l = (*l)->next;
				if ((compTail != NULL) && (compTail->pba < tl1->pba)) {
					compTail->next = tl1;
					tl1->next = NULL;
					compTail = tl1;
				}
				else *comp = insertPlNode(*comp, tl1);
				tl1 = *l;
			}
			else {
				tl2->next = tl1->next;
				if ((compTail != NULL) && (compTail->pba < tl1->pba)) {
					compTail->next = tl1;
					tl1->next = NULL;
					compTail = tl1;
				}
				else *comp = insertPlNode(*comp, tl1);
				tl1 = tl2->next;
			}
			modified = 1;
		}
		else {
			tl2 = tl1;
			tl1 = tl1->next;
		}
	}

	if (modified) {
		putBucket(b1->hb_num, b1);
	}

	return 0;
}

unsigned int HashBucket::updateBlocks(struct hashbucket *b1, struct pbaList ** l) {
	struct pbaList *comp = NULL, *tl1, *tl2, *compTail;
	struct hashbucket *b2; 
	unsigned int bno, retval = 0;

	updateBlocksBKT(b1, l, &comp);

	// Check if any more entries are there, which are not updated
	if (*l != NULL) {
		// Try to update in the entire chain of buckets
		// starting from the first bucket.
		bno = getFirstBucket(b1);

		while ((*l != NULL) && (bno != 0)) {
			if (bno == b1->hb_num) 
				bno = b1->hb_nextbucket; // Skip b1
			else {
				b2 = getBucket(bno);
				updateBlocksBKT(b2, l, &comp);
				// Already written if changed, so zero flag
				bno = b2->hb_nextbucket;
				releaseBucket(b2->hb_num, 0); 
			}
		}
	}

	// Go to the last node in the comp list
	compTail = NULL;
	tl1 = comp;

	while (tl1 != NULL) {
		compTail = tl1;
		tl1 = tl1->next;
	}

	// If still some more blocks remain, then add them
	
	if (*l != NULL) {
		addBlocks(b1, *l);

		tl1 = *l;
		while (tl1 != NULL) {
			tl2 = tl1;
			tl1 = tl1->next;
			if ((compTail != NULL) && (compTail->pba < tl2->pba)) {
				compTail->next = tl2;
				tl2->next = NULL;
				compTail = tl2;
			}
			else comp = insertPlNode(comp, tl2);
		}

		retval = 1;	// Some missing blocks???
	}

	// set the original list to completed list
	*l = comp;
	return retval;
}

// Add list of blocks
unsigned int HashBucket::addBlocks(struct hashbucket *b, pbaList *l) {
	struct hashbucket *b2;
	unsigned int bno2, bno;
	int comp = 0;
	int modified = 0;
	pbaList *cl1; 

	cl1 = l;
	while (cl1 != NULL) {
		if (addBlockBKT(b, cl1->fp, cl1->pba, cl1->refcount) == 1) {
			// Successfully added, bucket number is 
			// returned through refcount
			modified = 1;
			cl1 = cl1->next;
			comp++;
		}
		else break; // No more space, in the current bucket
	}

	if (modified) putBucket(b->hb_num, b);

	if (cl1 != NULL) {
		// Some more blocks are pending, start from the
		// first bucket in the chain and add the blocks to the 
		// available buckets

		bno2 = bno = getFirstBucket(b);
	
		while ((cl1 != NULL) && (bno != 0)) {
			if (bno == b->hb_num) 
				bno = b->hb_nextbucket; // Skip b
			else {
				b2 = getBucket(bno);
				modified = 0;
				if (b2->hb_nent < BLKS_PER_HBKT) { // Not Full
					while (cl1 != NULL) {
						if (addBlockBKT(b2, cl1->fp, cl1->pba, cl1->refcount) == 1) {
							// Successfully added, bucket number is 
							// returned through refcount
							cl1 = cl1->next;
							comp++;
							modified = 1;
						}
						else break; // No more space, in the current bucket
					}
				}

				if (modified) putBucket(b2->hb_num, b2);
				bno2 = b2->hb_num; // Predecessor bucket 
				bno = b2->hb_nextbucket;
				releaseBucket(b2->hb_num, 0);
			}
		}

		// Check, if still some more blocks are not added
		while (cl1 != NULL) {
			// Attach a link bucket
			// Find a new bucket
			bno = findFreeHBucket();
			if (bno == 0) {
				// No free bucket found
				cout << "Error: No free bucket found\n";
				exit(0);
			}

			b2 = getBucket(bno2); // Last bucket in the chain
			b2->hb_nextbucket = bno;
			releaseBucket(b2->hb_num, CACHE_ENTRY_DIRTY);

			b2 = getBucket(bno);
			b2->hb_nent = 0;
			b2->hb_nextbucket = 0;
			b2->hb_prevbucket = bno2;

			while (cl1 != NULL) {
				if (addBlockBKT(b2, cl1->fp, cl1->pba, cl1->refcount) == 1) {
					// Successfully added, bucket number is 
					// returned through refcount
					cl1 = cl1->next;
					comp++;
				}
				else break; // No more space, in the current bucket
			}
			bno2 = bno;
			releaseBucket(bno, CACHE_ENTRY_DIRTY);
		}
	}
		
	return comp;	// Count of entries added	
}

// Search for one block in bucket b
unsigned int HashBucket::searchForPbaBKT(struct hashbucket *b, 
		unsigned int pba, unsigned char hash[], 
		unsigned int *refcount) {
        unsigned int j;
        unsigned int done = 0;

        // update in the current bucket b
        for (j=0; j<b->hb_nent; j++) {
                // Search for the pba
                if (b->hb_pba[j] == pba) {
                        memcpy(hash, b->hb_fp[j], 16);
                        *refcount = b->hb_pbaref[j];
                        done = 1;
                        break;
                }
        }

        return done;
}

unsigned int HashBucket::searchForPba(struct hashbucket *b, 
		unsigned int pba, unsigned char * hash, 
		unsigned int *vbno, unsigned int *refcount) {
	struct hashbucket *b2;
	unsigned int bno, bno2, fb; 
	unsigned int done = 0;

	*vbno = 0;

	// Search in the current bucket
	if (searchForPbaBKT(b, pba, hash, refcount) == 1) {
		if (b->hb_num <= HASH_BUCK_COUNT)
			*vbno = b->hb_num;
		else 	*vbno = getFirstBucket(b);
		return 1;
	}

	// If not found, search the entire chain of buckets
	fb = bno2 = getFirstBucket(b);
	bno = b->hb_num;

	while ((bno2 != 0) && (!done)){
		if (bno2 == bno) // current bucket?
			bno2 = b->hb_nextbucket;
		else {
			b2 = getBucket(bno2);

			done = searchForPbaBKT(b2, pba, hash, refcount);
			if (done) *vbno = fb;

			bno2 = b2->hb_nextbucket;
			releaseBucket(b2->hb_num, 0);
		}
	}

	return done;
}


pbaList * HashBucket::plalloc(void) { 
	pbaList *p;
	BKTVal *b;

	p = PFL; 
	while (p == NULL) {
		// Find a victim and free the block
		b = KVTail;

		bvdelete(b);
		bvflush(b);
		b->next = FL;
		FL = b;

		p = PFL; 
	}

	if (PFL != NULL) PFL = PFL->next; 

	return p;
}

pbaList * HashBucket::plsearchp(BKTVal *b, unsigned int pba) {
	pbaList *p; 

	p = b->val; 
	while ((p != NULL) && (p->pba != pba)) 
		p = p->next; 

	return p;
}

pbaList * HashBucket::plsearchh(BKTVal *b, unsigned char *hash) {
	pbaList *p;

	p = b->val; 
	while ((p != NULL) && (memcmp(p->fp, hash, 16) != 0)) 
		p = p->next;

	return p;
}

void HashBucket::pldelete(BKTVal *b, pbaList *p) {
	pbaList *pred, *p1; 

	pred = NULL;
	p1 = b->val; 
	while ((p1 != NULL) && (p1 != p)) {
		pred = p1;
		p1 = p1->next; 
	}

	if (pred != NULL) pred->next = p->next;
	else b->val = p->next;
}

BKTVal *HashBucket::bvalloc(void) { 
	BKTVal *b;

	b = FL; 
	if (FL != NULL) FL = FL->next; 

	if (b == NULL) {
		b = KVTail;
		bvdelete(b);
		bvflush(b);
	}

	return b;
}

void HashBucket::bvdelete(BKTVal *b) { 
	unsigned int i; 

	i = b->key % KVHASH; 
	if (b->next != NULL) (b->next)->prev = b->prev; 
	if (b->prev != NULL) (b->prev)->next = b->next;
	else KVQ[i] = b->next; 
	if (b->tnext != NULL) (b->tnext)->tprev = b->tprev;
	else KVTail = b->tprev; 
	if (b->tprev != NULL) (b->tprev)->tnext = b->tnext;
	else KVHead = b->tnext; 
}

void HashBucket::bvprepend(BKTVal *b) { 
	int i; 

	i = b->key % KVHASH;
	b->prev = NULL;
	b->next = KVQ[i];
	if (KVQ[i] != NULL) KVQ[i]->prev = b;
	KVQ[i] = b;
	b->tprev = NULL;
	b->tnext = KVHead;
	if (KVHead != NULL) {
		KVHead->tprev = b;
		KVHead = b;
	}
	else KVHead = KVTail = b;
}

BKTVal * HashBucket::bvsearch(unsigned int bno) { 
	BKTVal *b;
	unsigned int i;

	i = bno % KVHASH; 
	b = KVQ[i]; 
	while ((b != NULL) && (b->key != bno))
		b = b->next;

	return b;
}

void HashBucket::bvflush(BKTVal *bv) {
	hashbucket *b;
	pbaList *p1, *p2, *al, *ul;
	unsigned int vbno, ucount = 0;
       
	vbno = bv->key; 

	b = getBucket(vbno); 
 
	p1 = bv->val; 
	al = ul = NULL; 

	// Separate the list of entries into add list and update list
	while (p1 != NULL) { 
		p2 = p1; 
		p1 = p1->next; 
 
		if (p2->flags & CACHE_ENTRY_DIRTY) { 
			if (p2->flags & CACHE_ENTRY_COPY) { 
				ul = insertPlNode(ul, p2);
				ucount++;
			} 
			else { 
				al = insertPlNode(al, p2);
			}
		}
		else {
			p2->next = PFL;
			PFL = p2;
		}
	}

	// Update blocks
	if (ul != NULL) { 
		updateBlocks(b, &ul); 
		BKTUPDATE += ucount; 
	}

	// Add blocks
	if (al != NULL) {
		addBlocks(b, al);
	}

	releaseBucket(b->hb_num, CACHE_ENTRY_DIRTY);

       	// Free the pba list entries	
	while (ul != NULL) { 
		p1 = ul; 
		ul = ul->next; 
		p1->next = PFL;
		PFL = p1;
	} 

       // Free the pba list entries	
	while (al != NULL) { 
		p1 = al;
		al = al->next;
		p1->next = PFL;
		PFL = p1;
	}
}


pbaList * HashBucket::bvinsertval(BKTVal *bv, unsigned int pba, 
		unsigned char *hash, unsigned int refcount, 
		unsigned short flg) { 
	pbaList *p; 

	// Special case
	if ((KVTail == NULL) && (PFL == NULL)) {
		bvflush(bv);
		bv->val = NULL;
	}
	// Allocate pbaList structure, and add tht in order
	p = plalloc();

	p->pba = pba;
	memcpy(p->fp, hash, 16);
	p->refcount = refcount;
	p->flags = flg;

	bv->val = insertPlNode(bv->val, p);

	return p;
}


int HashBucket::incrementRefValWrap(unsigned int bno, unsigned int pba) {
	struct BKTVal *c; 
	pbaList *pl;
	unsigned int vbno, ref;
	int refcount = 0;
	unsigned char hash[16];
	struct hashbucket *b;

	BKTUPDATE++;
	c = bvsearch(bno);
	if (c != NULL) {
		pl = plsearchp(c, pba);

		if (pl != NULL) {
			// Entry found in cache
			pl->refcount++;
			refcount = pl->refcount;
			pl->flags |= CACHE_ENTRY_DIRTY;
			bvdelete(c);
			bvprepend(c);
			return refcount;
		}
	}

	// Entry not found, so search the bucket
	b = getBucket(bno);
	searchForPba(b, pba, hash, &vbno, &ref);
	releaseBucket(bno, 0);
	if (vbno != 0) {
		// Entry found in the bucket
		// Create a cached entry, if necessary
		++ref;
		refcount = (int) ref;
		pl = insertValWrap(vbno, pba, hash, refcount);
		pl->flags = CACHE_ENTRY_DIRTY | CACHE_ENTRY_COPY;
	}
	else {
		refcount = -1;
		cout << "Error: entry not found 6\n";
		exit(0);
	}

	return refcount;
}

int HashBucket::incrementRefValWrap(unsigned char *hash, unsigned int pba) {
	unsigned int bno;

	bno = (hash[0] | (hash[1] << 8)) + 1;

	return incrementRefValWrap(bno, pba);

}

int HashBucket::decrementRefValWrap(unsigned int bno, unsigned int pba) {
	struct BKTVal *c; 
	pbaList *pl; 
	unsigned int vbno, ref;
	int refcount;
	unsigned char hash[16];
	struct hashbucket *b;

	BKTUPDATE++;
	c = bvsearch(bno);
	if (c != NULL) {
		pl = plsearchp(c, pba);

		if (pl != NULL) {
			pl->refcount--;
			refcount = pl->refcount;
			if (refcount == 0) {
				// Remove the entry
				pldelete(c, pl);
				if (c->val == NULL) {
					bvdelete(c);
					c->next = FL;
					FL = c;
					c = NULL;
				}

				if (pl->flags & CACHE_ENTRY_COPY) {
					deletePba(bno, pba);
				}
				pl->next = PFL;
				PFL = pl;
			}
			else pl->flags |= CACHE_ENTRY_DIRTY;

			if (c != NULL) {
				bvdelete(c);
				bvprepend(c);
			}
			return refcount;
		}
	}

	// Entry not found, so search the bucket
	b = getBucket(bno);
	searchForPba(b, pba, hash, &vbno, &ref);
	releaseBucket(bno, 0);
	if (vbno != 0) {
		// Entry found in the bucket
		// Create a cached entry, if necessary
		--ref;
		refcount = (int) ref;
		if (refcount > 0) {
			pl = insertValWrap(vbno, pba, hash, refcount); 
			pl->flags = CACHE_ENTRY_DIRTY | CACHE_ENTRY_COPY;
		}
		else {
			deletePba(vbno, pba);
		}
	}
	else {
		refcount = -1;
		cout << "Error: entry not found 5\n";
		assert(refcount != -1);
		exit(0);
	}

	return refcount;
}


int HashBucket::getRefCountWrap(unsigned int bno, unsigned int pba) {
	struct BKTVal *c;
	pbaList *pl;
	unsigned int vbno, ref;
	int refcount = 0;
	unsigned char hash[16];
	struct hashbucket *b;

	BKTSEARCH++;
	c = bvsearch(bno);
	if (c != NULL) {
		pl = plsearchp(c, pba);
		if (pl != NULL) {
			bvdelete(c);
			bvprepend(c);

			return pl->refcount;
		}
	}

	// Entry not found, so search the bucket
	b = getBucket(bno);
	searchForPba(b, pba, hash, &vbno, &ref);
	releaseBucket(bno, 0);
	if (vbno != 0) {
		// Entry found in the bucket
		// Create a cached entry, if necessary
		refcount = (int) ref;
		pl = insertValWrap(vbno, pba, hash, ref);
		pl->flags = CACHE_ENTRY_CLEAN | CACHE_ENTRY_COPY;
	}

	return refcount;
}


int HashBucket::decrementRefValWrap(unsigned char *hash, unsigned int pba) {
	unsigned int bno;

	bno = (hash[0] | (hash[1] << 8)) + 1;
	return decrementRefValWrap(bno, pba);
}

int HashBucket::searchValWrap(unsigned char *hash, unsigned int *pba,
		unsigned int *ref) {
	int found = 0;
	struct hashbucket *b; 
	unsigned int bno, bno1;

	*pba = 0;
	*ref = 0;

	BKTSEARCH++;
	bno = (hash[0] | (hash[1] << 8)) + 1;

	b = getBucket(bno);
	found = searchForHash(b, hash, pba, &bno1, ref);
	releaseBucket(bno, 0);

	return found;
}

pbaList * HashBucket::insertValWrap(unsigned int bno, 
		unsigned int pba, unsigned char hash[16], 
		unsigned int refcount) {
        struct BKTVal *c;
        pbaList *pl1; 

        assert((bno > 0) && (pba > 0));

	BKTINSERT++;

        c = bvsearch(bno);

        if (c == NULL) {
                c = bvalloc();
		c->key = bno;
		c->val = NULL;
        }
	else bvdelete(c);

        pl1 = bvinsertval(c, pba, hash, refcount, CACHE_ENTRY_DIRTY);
        bvprepend(c);

        return pl1;
}

void HashBucket::flushValWrap(void) {
        struct BKTVal *c; 

        while (KVTail != NULL) {
                c = KVTail;
                bvflush(c);
                bvdelete(c);
		c->next = FL;
		FL = c;
        }
}




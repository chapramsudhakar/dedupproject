#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "bucket.h"
#include "cachemem.h"
#include "metadata.h"
#include "util.h"

extern unsigned long long int curTime;
extern unsigned long long int UNIQUE_BLOCK_COUNT;
extern void getPbaRefHash(unsigned int pba, unsigned char h[]);
extern int OPERATION;
//extern MDMaps *md;
extern Bucket *bkt;

Bucket::Bucket(const char *bktfname, const char *bktidxfname, unsigned int btmax, unsigned int bthash, unsigned int inc,
		StorageNode *sn) {
	//string fname = "buckets.dat";
	bucket b;

	// Open the buckets database file
	bfs.open(bktfname, ios::in | ios:: out | ios::binary);
	if (bfs.fail()) {
		cout << "Buckets file opening failure : " << bktfname << endl;
		exit(0);
	}
	bfs.clear();
	b_sn = sn;

	// Initailize the bucket index and bucket cache
#ifdef	DARCP
	bc = new DARC(bktfname, BUCKET_CACHE_LIST, BUCKET_CACHE_SIZE, 512, b_sn);
#else
	bc = new LRU(bktfname, BUCKET_CACHE_LIST, BUCKET_CACHE_SIZE, b_sn);
#endif

	bi = new IBTree(bktidxfname, btmax, bthash, 100, b_sn);
	assert(bi != NULL);
	bi->enableCache(BUCKETINDEX_CACHE_SIZE);
	bi->enableKeyValCache(RID_CACHE_SIZE);

	maxbuckets = MAX_BUCKETS;
	incr = inc;
	BUCKET_READ_COUNT = 0;
	BUCKET_READ_CACHEHIT = 0;
	BUCKET_WRITE_COUNT = 0;
	BUCKET_WRITE_CACHEHIT = 0;
	BUCKET_EFFECTIVE_WRITES = 0;
	BUCKET_FLUSH_WRITES = 0;
	BUCKET_USED_COUNT = 0;

	BUCKET_READ_COUNT_READ = 0;
	BUCKET_WRITE_COUNT_READ = 0;
	BUCKET_READ_COUNT_WRITE = 0;
	BUCKET_WRITE_COUNT_WRITE = 0;
	BKTSEARCH = 0;
	BKTINSERT = 0;
	BKTUPDATE = 0;
	BKTDELETE = 0;
	
	nextFreeBucket = 1;

	flist = new FreeList("bucket",512);

	// Get max buckets and used buckets from 
	// 0'th bucket
	bfs.seekg(0);
	bfs.read((char *)&b, sizeof(bucket));
	maxbuckets = b.b_nextbucket;
	BUCKET_USED_COUNT = b.b_prevbucket;

	//cout << bktfname << ": max buckets : " << maxbuckets << " used " << BUCKET_USED_COUNT << endl;
	// Find the current total number of buckets 
	//bfs.seekg(0, bfs.end);
	//maxbuckets = bfs.tellg()/sizeof(struct bucket);
}

// Initialize key value cache
void Bucket::enableKeyValueCache(unsigned int plsize, unsigned int bcount) {
	unsigned int i, n;
	struct BKTVal *bv1, *bv2;
	struct pbaList *pl1, *pl2;

	KVCacheSize = bcount * sizeof(struct BKTVal);
	KVHASH = bcount/20 + 1;

	//cout << "Bucket plsize " << plsize << " bcount " << bcount << " KVHASH " << KVHASH << endl;
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
}

void Bucket::displayStatistics(void) {
	cout << "Bucket structure stats:\nBucket Index\n";
	bi->displayStatistics();
	cout << "Bucket Cache\n";
	bc->displayStatistics();
	cout << "BUCKET_READ_COUNT \t\t= " << BUCKET_READ_COUNT << endl
		<< "BUCKET_READ_CACHEHIT \t\t= " << BUCKET_READ_CACHEHIT << endl
		<< "BUCKET_WRITE_COUNT \t\t= " << BUCKET_WRITE_COUNT << endl
		<< "BUCKET_WRITE_CACHEHIT \t\t= " << BUCKET_WRITE_CACHEHIT << endl
		<< "BUCKET_EFFECTIVE_READS \t\t= " << (BUCKET_READ_COUNT - BUCKET_READ_CACHEHIT)  << endl
		<< "BUCKET_EFFECTIVE_WRITES \t= " << BUCKET_EFFECTIVE_WRITES << endl
		<< "BUCKET_FLUSH_WRITES \t= " << BUCKET_FLUSH_WRITES << endl
		<< "Bucket read operation read count \t=" << BUCKET_READ_COUNT_READ << endl
		<< "Bucket read operation write count \t=" << BUCKET_WRITE_COUNT_READ << endl
		<< "Bucket write operation read count \t=" << BUCKET_READ_COUNT_WRITE << endl
		<< "Bucket write operation write count \t=" << BUCKET_WRITE_COUNT_WRITE << endl
		<< "BKTINSERT \t\t\t=" << BKTINSERT << endl
		<< "BKTSEARCH \t\t\t=" << BKTSEARCH << endl
		<< "BKTUPDATE \t\t\t=" << BKTUPDATE << endl
		<< "BKTDELETE \t\t\t=" << BKTDELETE << endl
		<< "BUCKET_USED_COUNT \t\t= " << BUCKET_USED_COUNT << endl
		<< "Maximum Buckets \t\t= " << maxbuckets << endl;
}

struct bucket * Bucket::getBucket(unsigned int n) {
	CacheEntry * ce;
	struct bucket *b1;
	unsigned int wrval;

	assert(n > 0);

	// Find the bucket in the cache
	ce = bc->searchCache(n);
	if (ce != NULL)  {  // Entry found in the cache
		BUCKET_READ_COUNT++;
		BUCKET_READ_CACHEHIT++;
		ce->ce_rtime = curTime;
		ce->ce_refcount++;
		bc->c_rrefs++;
		bc->repositionEntry(ce);
		b1 = (struct bucket *)ce->ce_object;
	}
	else {
		// Read block and add to the cache
		b1 = (struct bucket *) b_sn->mem->b512malloc();

		assert(b1 != NULL);
		assert(readBucket(n, b1) == 0);

		wrval = bc->addReplaceEntry(bfs, n, (void *)b1, 
			sizeof(struct bucket), curTime, 0, METADATADISK, 
			CACHE_ENTRY_CLEAN, NULL, &ce);
		BUCKET_EFFECTIVE_WRITES += wrval;
		if (OPERATION == OPERATION_READ)
			BUCKET_WRITE_COUNT_READ += wrval;
		else
			BUCKET_WRITE_COUNT_WRITE += wrval;
	}

	lockEntry(ce);

	return b1;
}

void Bucket::releaseBucket(unsigned int n, unsigned short dirty) {
	CacheEntry *c;

	c = bc->searchCache(n);

	if (dirty != 0) {
		if (!(c->ce_flags & CACHE_ENTRY_DIRTY))
			bc->c_dcount++;
		c->ce_flags |= dirty;
		BUCKET_WRITE_COUNT++;
		BUCKET_WRITE_CACHEHIT++;
	}

	unlockEntry(c);
}

int Bucket::putBucket(unsigned int n, struct bucket *b) {
	CacheEntry * ce;
	struct bucket *b1;
	unsigned int wrval;

	assert(n > 0);
	//assert(n == b->b_num);
	BUCKET_WRITE_COUNT++;
	// Find the bucket in the cache and copy into that
	ce = bc->searchCache(n);
	if (ce != NULL)  {  // Entry found in the cache
		lockEntry(ce);
		BUCKET_WRITE_CACHEHIT++;
		if (ce->ce_object != b)
			memcpy(ce->ce_object, (char *)b, sizeof(struct bucket));

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
		b1 = (struct bucket *) b_sn->mem->b512malloc();

		assert(b1 != NULL);
		memcpy((char *)b1, (char *)b, sizeof(struct bucket));	

		wrval = bc->addReplaceEntry(bfs, n, (void *)b1, sizeof(struct bucket), 0, curTime, METADATADISK, CACHE_ENTRY_DIRTY, NULL, &ce);
		BUCKET_EFFECTIVE_WRITES += wrval;
		if (OPERATION == OPERATION_READ)
			BUCKET_WRITE_COUNT_READ += wrval;
		else
			BUCKET_WRITE_COUNT_WRITE += wrval;
	}

	return 0;
}

// Read bucket number n
int Bucket::readBucket(unsigned int n, struct bucket *b) {

	assert((n > 0) && (n <= maxbuckets));

	BUCKET_READ_COUNT++;

	if (OPERATION == OPERATION_READ)
		BUCKET_READ_COUNT_READ++;
	else
		BUCKET_READ_COUNT_WRITE++;

	// Seek to the required position in the bucket file
	// and read the structure.
	bfs.seekg((unsigned long)(n-1)*sizeof(struct bucket));
	if (bfs.fail()) {
		cout << "Error : readBucket seek failed\n";
		bfs.clear();
		return -1;
	}

	bfs.read((char *)b, sizeof(struct bucket));
	DiskRead(b_sn, curTime, METADATADISK, n, 1);
	
	if (bfs.fail()) {
		cout << n << " Error : readBucket read failed\n";
		bfs.clear();
		return -1;
	}

	return 0;
}
/*
int Bucket::evictor(fstream &fs, CacheEntry *ent, int disk) {
        struct bucket *b;

        b = (struct bucket *)ent->ce_object;
        //cout << "Evicting key " << ent->ce_key << " b_num " << b->hb_num << endl;
        //assert(ent->ce_key == b->b_num);
        bkt->writeBucket(ent->ce_key, b);
        return 0;
}
*/

// Write bucket number n
int Bucket::writeBucket(unsigned int n, struct bucket *b) {

	assert((n > 0) && (n <= maxbuckets));

	//assert(n == b->b_num);
	BUCKET_WRITE_COUNT++;

	if (OPERATION == OPERATION_READ)
		BUCKET_WRITE_COUNT_READ++;
	else
		BUCKET_WRITE_COUNT_WRITE++;

	// Seek to the required position in the bucket file
	// and write the structure.
	bfs.seekp((unsigned long)(n-1)*sizeof(struct bucket));
	if (bfs.fail()) {
		cout << "Error : readBucket seek failed\n";
		bfs.clear();
		return -1;
	}

	bfs.write((char *)b, sizeof(struct bucket));
	DiskWrite(b_sn, curTime, METADATADISK, n, 1);
	if (bfs.fail()) {
		cout << n << " Error : readBucket read failed\n";
		bfs.clear();
		return -1;
	}

	return 0;
}

// Flush the bucket cache and bucket index cache
void Bucket::flushCache(void) {
	unsigned int wrcount;
	bucket b;

	wrcount = bc->flushCache(bfs, sizeof(struct bucket), METADATADISK);
	BUCKET_EFFECTIVE_WRITES += wrcount;
	BUCKET_FLUSH_WRITES += wrcount;
	bi->flushCache(METADATADISK);
	// Write the first bucket info.
	memset((char *)&b, 0, sizeof(bucket));
	b.b_nextbucket = maxbuckets;
	b.b_prevbucket = BUCKET_USED_COUNT;
	bfs.seekp(0);
	bfs.write((char *)&b, sizeof(bucket));
}

// Find the matching bucket for rid value
unsigned int Bucket::findBucket(unsigned char * rid) {
	unsigned int bno;
	unsigned int ridno;

	//ridno = rid[0] | (rid[1] << 8) | ((rid[2] << 16) & 0x0f) ;
	ridno = rid[0] | (rid[1] << 8) | (rid[2] << 16) | (rid[3] << 24);

	// Search the index file and return the bucket number
	bi->searchValWrap(ridno, &bno);

	// Found/not found (zero) the bucket number
	return bno;
}

unsigned int Bucket::isPbaListEmpty(unsigned int bno) {
	BKTVal *c;

	// First search Bucket cache
	c = bvsearch(bno + HASH_BUCK_COUNT);
	if (c != NULL) return 0;
	else return 1;
}

pbaList * Bucket::plalloc(void) { 
	pbaList *p;
	BKTVal *b;

	p = PFL; 

	while (p == NULL) {
		// Find a victim and free the bucket, list
		b = KVTail;

		bvdelete(b);
		bvflush(b);
		b->next = FL;
		FL = b;

		p = PFL; 
	}

	PFL = PFL->next; 

	return p;
}

pbaList * Bucket::plsearchp(BKTVal *b, unsigned int pba) {
	pbaList *p; 

	p = b->val; 
	while ((p != NULL) && (p->pba != pba)) 
		p = p->next; 

	return p;
}

pbaList * Bucket::plsearchh(BKTVal *b, unsigned char *hash) {
	pbaList *p;

	p = b->val; 
	while ((p != NULL) && (memcmp(p->fp, hash, 16) != 0)) 
		p = p->next;

	return p;
}

void Bucket::pldelete(BKTVal *b, pbaList *p) {
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

BKTVal *Bucket::bvalloc(void) { 
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

void Bucket::bvdelete(BKTVal *b) { 
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

void Bucket::bvprepend(BKTVal *b) { 
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

BKTVal * Bucket::bvsearch(unsigned int bno) { 
	BKTVal *b;
	unsigned int i;

	i = bno % KVHASH; 
	b = KVQ[i]; 
	while ((b != NULL) && (b->key != bno))
		b = b->next;

	return b;
}

void Bucket::bvflush(BKTVal *bv) {
	bucket *b;
	pbaList *p1, *p2, *al, *ul;
	unsigned int vbno, ucount = 0;
      
	vbno = bv->key; 

	b = getBucket(vbno - HASH_BUCK_COUNT); 
 
	p1 = bv->val; 
	al = ul = NULL; 

	// Separate the list of entries into add list and update list
	while (p1 != NULL) { 
		p2 = p1; 
		p1 = p1->next; 
 
		if (p2->flags & CACHE_ENTRY_DIRTY) { 
			if (p2->flags & CACHE_ENTRY_COPY) { 
				p2->next = ul; 
				ul = p2; 
				ucount++;
			} 
			else { 
				p2->next = al; 
				al = p2;
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

	releaseBucket(b->b_num, CACHE_ENTRY_DIRTY);

       	// Free the pba list entries	
	while (ul != NULL) { 
		// After returning from addBlocks/updateBlocks,
		// refcount is modified as bucket number. Use that
		// value check if pba-to-bucket map is to be updated
		if (vbno != (ul->refcount + HASH_BUCK_COUNT)) 
			b_sn->md->pb->updateValWrap(ul->pba, ul->refcount + HASH_BUCK_COUNT); 

		p1 = ul; 
		ul = ul->next; 
		p1->next = PFL;
		PFL = p1;
	}

       // Free the pba list entries	
	while (al != NULL) { 
		if (vbno != (al->refcount + HASH_BUCK_COUNT))  
			// After returning from addBlocks/updateBlocks,
			// refcount is modified as bucket number. Use that
			// value check if pba-to-bucket map is to be updated
			b_sn->md->pb->updateValWrap(al->pba, al->refcount + HASH_BUCK_COUNT); 

		p1 = al;
		al = al->next;
		p1->next = PFL;
		PFL = p1;
	}
}


pbaList * Bucket::bvinsertval(BKTVal *bv, unsigned int pba, unsigned char *hash,
		unsigned int refcount, unsigned short flg) { 
	pbaList *p;

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

// Find a free bucket
unsigned int Bucket::findFreeBucket() {
	unsigned int i;
	unsigned int lastBucket; 
	unsigned int olastBucket;
	struct bucket *b1, b;
	CacheEntry *c;

	lastBucket = nextFreeBucket;
	if (lastBucket > maxbuckets) lastBucket = 2;
	olastBucket = lastBucket;
	BUCKET_USED_COUNT++;


	if ((i = flist->getVal()) > 0) {
		return i;
	}

	for (i=lastBucket; i<= maxbuckets; i++) {
		c = bc->lookCache(i);

		if (c != NULL) b1 = (bucket *)c->ce_object;
		else {
			assert(readBucket(i, &b) == 0);
			b1 = &b;
		}

		if (b1->b_padding[0] == 0) {
			nextFreeBucket = i+1;
			return i;
		}
	}

	if ((BUCKET_USED_COUNT + 40) < maxbuckets) {
		for (i=2; i<olastBucket; i++) {
			c = bc->lookCache(i);

			if (c != NULL) b1 = (bucket *)c->ce_object;
			else {
				assert(readBucket(i, &b) == 0);
				b1 = &b;
			}

			if (b1->b_padding[0] == 0) {
				nextFreeBucket = i+1;
				return i;
			}
		}
	}

	// No free bucket found, so expand the list of buckets
	bfs.seekp(0, bfs.end);
	memset(&b, 0, sizeof(struct bucket));
	for (i=1; i<=incr; i++) {
		b.b_num = maxbuckets+i;
		bfs.write((char *)&b, sizeof(struct bucket));
	}

	i = maxbuckets+1;
	nextFreeBucket = i+1;
	maxbuckets += incr;
	return i;
}

// Add one block to the bucket b
unsigned int Bucket::addBlockBKT(struct bucket *b, unsigned int pba,
		unsigned char *hash, unsigned int refcount) {
	unsigned int done = 0;

	if ((b->b_nent + 1) <= BLKS_PER_BKT) {
		b->b_pba[b->b_nent] = pba;
		memcpy(b->b_fp[b->b_nent], hash, 16);
		b->b_refcount[b->b_nent] = refcount;
		b->b_nent++;
		putBucket(b->b_num, b);
		done = 1;
	}

	return done;
}

// Delete one block from bucket b
unsigned int Bucket::deleteBlockBKT(struct bucket *b, unsigned int pba) {
	unsigned int j, k;
	unsigned int done = 0;

	// Search for the pba
	for (j=0; j<b->b_nent; j++) {
		if (b->b_pba[j] == pba) {
			// Found, shift next entries
			k = j+1;
			while (k < b->b_nent) {
				b->b_pba[k-1] = b->b_pba[k];
				memcpy(b->b_fp[k-1], b->b_fp[k], 16);
				b->b_refcount[k-1] = b->b_refcount[k];
				k++;
			}
			b->b_nent--;
			putBucket(b->b_num, b);	
			BKTDELETE++;
			done = 1;
			break;
		}
	}
	
	return done;
}	

// Delete bucket b from chain of buckets 
unsigned int Bucket::deleteBKT(struct bucket *b) {
	struct bucket *b1;
	unsigned int bno1, bno2;

	if ((b->b_nent == 0)  && (isPbaListEmpty(b->b_num) == 1)) {
		b->b_padding[0] = 0; // free
		flist->freeVal(b->b_num);
		// Bucket is empty, so it should be 
		// deleted from the list of buckets
		BUCKET_USED_COUNT--;
		bno1 = b->b_prevbucket;
		bno2 = b->b_nextbucket;

		// delete/update bucket index entry
		if ((bno1 == 0) && (bno2 == 0))
			bi->deleteValWrap(b->b_rid);
		else if (bno1 == 0) 
			bi->updateValWrap(b->b_rid, bno2);
			
		// Update linked list	
		if (bno1 != 0) {
			b1 = getBucket(bno1);
			b1->b_nextbucket = bno2;
			releaseBucket(bno1, CACHE_ENTRY_DIRTY);
		}

		if (bno2 != 0) {
			b1 = getBucket(bno2);
			b1->b_prevbucket = bno1;
			releaseBucket(bno2, CACHE_ENTRY_DIRTY);
		}
		putBucket(b->b_num, b);
		return 1;
	}
	return 0;
}

// Update one block in bucket b
unsigned int Bucket::updateBlockBKT(struct bucket *b, unsigned int pba,
		unsigned char *hash, unsigned int refcount) {
	unsigned int j;
	unsigned int done = 0;

	// update in the current bucket b
	for (j=0; j<b->b_nent; j++) {
		// Search for the pba
		if (b->b_pba[j] == pba) {
			memcpy(b->b_fp[j], hash, 16);
			b->b_refcount[j] = refcount;
			putBucket(b->b_num, b);
			done = 1;
			break;
		}
	}

	return done;
}

// Search for one block in bucket b
unsigned int Bucket::searchForPbaBKT(struct bucket *b, unsigned int pba,
		unsigned char hash[], unsigned int *refcount) {
	unsigned int j;
	unsigned int done = 0;

	// update in the current bucket b
	for (j=0; j<b->b_nent; j++) {
		// Search for the pba
		if (b->b_pba[j] == pba) {
			memcpy(hash, b->b_fp[j], 16);
			*refcount = b->b_refcount[j];
			done = 1;
			break;
		}
	}

	return done;
}

// Search for one hash in bucket b
unsigned int Bucket::searchForHashBKT(struct bucket *b, unsigned char hash[],
		unsigned int *pba, unsigned int *refcount) {
	unsigned int j;
	unsigned int done = 0;

	// update in the current bucket b
	for (j=0; j<b->b_nent; j++) {
		// Search for the pba
		if (memcmp(b->b_fp[j], hash, 16) == 0) {
			*pba = b->b_pba[j];
			*refcount = b->b_refcount[j];
			done = 1;
			break;
		}
	}

	return done;
}

// Get first bucket in the chain of buckets, private
unsigned int Bucket::getFirstBucket(struct bucket *b) {
	unsigned int bno, retval;

	retval = b->b_num;
	bno = b->b_prevbucket;
	while (bno != 0) {
		b = getBucket(bno);
		retval = bno;
		bno = b->b_prevbucket;
		releaseBucket(retval, 0);
	}

	return retval;
}

unsigned int Bucket::getFirstBucketWrap(unsigned int bno) {
	struct bucket *b;
	unsigned int fbno;

	b = getBucket(bno - HASH_BUCK_COUNT);
	fbno = getFirstBucket(b) + HASH_BUCK_COUNT;
	releaseBucket(bno - HASH_BUCK_COUNT, 0);

	return fbno;
}

// Add a new bucket with rid and one segment
unsigned int Bucket::addNewBucket(unsigned char * rid) {
	unsigned int bno;
	struct bucket *b1;

	// Find a new bucket
	bno = findFreeBucket();
	if (bno == 0) {
		// No free bucket found
		cout << "Error: No free bucket found\n";
		exit(0);
	}
	
	b1 = getBucket(bno);


	b1->b_padding[0] = 1;	// Used bucket
	//b1->b_rid = rid[0] | (rid[1] << 8) | ((rid[2] << 16) & 0x0f);
	b1->b_rid = rid[0] | (rid[1] << 8) | (rid[2] << 16) | (rid[3] << 24);
	b1->b_nent = 0;
	b1->b_nextbucket = 0;
	b1->b_prevbucket = 0;

	bi->insertValWrap(b1->b_rid, bno);

	releaseBucket(bno, CACHE_ENTRY_DIRTY);

	return bno;
}

// Update the entries within the bucket b1
unsigned int Bucket::updateBlocksBKT(struct bucket *b1, struct pbaList ** l,
		struct pbaList **comp) {
	int modified = 0;
	pbaList *tl1, *tl2; 
	pbaList *compTail;

	// Go to the tail end of comp list
	compTail = NULL;
	tl1 = *comp;
	while (tl1 != NULL) {
		compTail = tl1;
		tl1 = tl1->next;
	}

	tl1 = *l;
	tl2 = NULL; // Predcessor

	while (tl1 != NULL) {
		if (updateBlockBKT(b1, tl1->pba, tl1->fp, tl1->refcount) == 1) {
			// Updated, so remove it and add to completed list
			if (tl2 == NULL) {
				tl1->refcount = b1->b_num;
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
				tl1->refcount = b1->b_num;
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

	if (modified) putBucket(b1->b_num, b1);

	return 0;
}

// Update blocks in the chain of buckets b1
unsigned int Bucket::updateBlocks(struct bucket *b1, struct pbaList ** l) {
	struct pbaList *comp = NULL, *compTail, *tl1, *tl2;
	struct bucket *b2; 
	unsigned int bno, retval = 0;

	updateBlocksBKT(b1, l, &comp);

	// Check if any more entries are there, which are not updated
	if (*l != NULL) {
		// Try to update in the entire chain of buckets
		// starting from the first bucket.
		bno = getFirstBucket(b1);

		while ((*l != NULL) && (bno != 0)) {
			if (bno == b1->b_num) 
				bno = b1->b_nextbucket; // Skip b1
			else {
				b2 = getBucket(bno);
				updateBlocksBKT(b2, l, &comp);
				bno = b2->b_nextbucket;

				// Already written if changed, so zero flag
				releaseBucket(b2->b_num, 0); 
			}
		}
	}

	// If still some more blocks remain, then add them
	// Go to the tail end of comp list
	compTail = NULL;
	tl1 = comp;
	while (tl1 != NULL) {
		compTail = tl1;
		tl1 = tl1->next;
	}

	
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

// Add the list of blocks to the bucket
unsigned int Bucket::addBlocks(struct bucket *b, pbaList * l) {
	struct bucket *b2, *b3; 
	unsigned int bno2, bno;
	int comp = 0;
	int modified = 0;
	pbaList *cl1; 

	cl1 = l;
	while (cl1 != NULL) {
		if (addBlockBKT(b, cl1->pba, cl1->fp, cl1->refcount) == 1) {
			// Successfully added, bucket number is 
			// returned through refcount
			cl1->refcount = b->b_num;
			modified = 1;
			cl1 = cl1->next;
			comp++;
		}
		else break; // No more space, in the current bucket
	}

	if (modified) putBucket(b->b_num, b);

	if (cl1 != NULL) {
		// Some more blocks are pending, start from the
		// first bucket in the chain and add the blocks to the 
		// available buckets

		bno2 = bno = getFirstBucket(b);
	
		while ((cl1 != NULL) && (bno != 0)) {
			if (bno == b->b_num) 
				bno = b->b_nextbucket; // Skip b
			else {
				b2 = getBucket(bno);
				modified = 0;
				if (b2->b_nent < BLKS_PER_BKT) { // Not Full
					while (cl1 != NULL) {
						if (addBlockBKT(b2, cl1->pba, cl1->fp, cl1->refcount) == 1) {
							// Successfully added, bucket number is 
							// returned through refcount
							cl1->refcount = b2->b_num;
							cl1 = cl1->next;
							comp++;
							modified = 1;
						}
						else break; // No more space, in the current bucket
					}
				}

				bno2 = b2->b_num; // Predecessor bucket 
				bno = b2->b_nextbucket;
				if (modified) 
					releaseBucket(b2->b_num, CACHE_ENTRY_DIRTY);
				else releaseBucket(b2->b_num, 0);
			}
		}

		// Check, if still some more blocks are not yet added
		while (cl1 != NULL) {
			// Attach a link bucket
			// Find a new bucket
			bno = findFreeBucket();
			if (bno == 0) {
				// No free bucket found
				cout << "Error: No free bucket found\n";
				exit(0);
			}

			b2 = getBucket(bno2); // Last bucket in the chain
			b2->b_nextbucket = bno;
			releaseBucket(b2->b_num, CACHE_ENTRY_DIRTY);
			b3 = getBucket(bno);
			// Add the segment to the new bucket and write it
			b3->b_padding[0] = 1; // used bucket
			b3->b_rid = b2->b_rid;
			b3->b_nent = 0;
			b3->b_nextbucket = 0;
			b3->b_prevbucket = bno2;
			putBucket(bno, b3); // Write the modified bucket

			b2 = b3;

			while (cl1 != NULL) {
				if (addBlockBKT(b2, cl1->pba, cl1->fp, cl1->refcount) == 1) {
					// Successfully added, bucket number is 
					// returned through refcount
					cl1->refcount = b2->b_num;
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

// Search for the hashes given in write request buffers and
// find their corresponding other details (pba's, bucket numbers, ref counts)
// This is called after wrap functions, so some of the entries might have been
// found already (indicated by non-zero pba[] values).
int Bucket::searchForHashes(struct bucket *b1, WriteReqBuf *w, 
		unsigned int seg_size, unsigned int pba[], 
		unsigned int bnos[], unsigned int refs[]) {
	struct bucket *b2; //, *b3;
	unsigned int k, bno, bno2;
	unsigned int count = 0;
	WriteReqBuf *t;

	// Remember the original bucket pointer
	bno = b1->b_num;

	// Count the already found entries
	for (k=0; k<seg_size; k++) 
		if (pba[k] != 0) count++;

	// Search with in the current bucket b1
	t = w;
	for (k=0; k<seg_size; k++) {
		if ((pba[k] == 0) && (searchForHashBKT(b1, t->wr_md5hash, 
					&pba[k], &refs[k]) == 1)) {
			count++;
			bnos[k] = b1->b_num;
		}
		t = t->wr_lbanext;
	}

	// If all found, return
	if (seg_size == count) return count;

	// If all are not found search the entire chain of buckets	
	bno2 = getFirstBucket(b1);
	
	while ((bno2 != 0) && (count < seg_size)) {
		if (bno2 == bno) // current bucket?
			bno2 = b1->b_nextbucket;
		else {
			b2 = getBucket(bno2);
			t = w;
			for (k=0; k<seg_size; k++) {
				if ((pba[k] == 0) && (searchForHashBKT(b2, 
					t->wr_md5hash, &pba[k], &refs[k]) == 1)) {
					count++;
					bnos[k] = b2->b_num;
				}
				t = t->wr_lbanext;
			}
			bno2 = b2->b_nextbucket;
			releaseBucket(b2->b_num, 0);
		}
	}

	return count;
}

// Search for the hashes given in list of hashes
// find their corresponding other details (pba's, bucket numbers, ref counts)
// This is called after wrap functions, so some of the entries might have been
// found already (indicated by non-zero pba[] values).
int Bucket::searchForHashes(struct bucket *b1, HashList *w, 
		unsigned int seg_size, unsigned int pba[], 
		unsigned int bnos[], unsigned int refs[]) {
	struct bucket *b2; 
	unsigned int k, bno, bno2;
	unsigned int count = 0;
	HashList *t;

	// Remember the original bucket pointer
	bno = b1->b_num;

	// Count the already found entries
	for (k=0; k<seg_size; k++) 
		if (pba[k] != 0) count++;

	// Search with in the current bucket b1
	t = w;
	for (k=0; k<seg_size; k++) {
		if ((pba[k] == 0) && (searchForHashBKT(b1, t->hl_hash, 
					&pba[k], &refs[k]) == 1)) {
			count++;
			bnos[k] = b1->b_num;
		}
		t = t->hl_next;
	}

	// If all found, return
	if (seg_size == count) return count;

	// If all are not found search the entire chain of buckets	
	bno2 = getFirstBucket(b1);
	
	while ((bno2 != 0) && (count < seg_size)) {
		if (bno2 == bno) // current bucket?
			bno2 = b1->b_nextbucket;
		else {
			b2 = getBucket(bno2);
			t = w;
			for (k=0; k<seg_size; k++) {
				if ((pba[k] == 0) && (searchForHashBKT(b2, 
					t->hl_hash, &pba[k], &refs[k]) == 1)) {
					count++;
					bnos[k] = b2->b_num;
				}
				t = t->hl_next;
			}
			bno2 = b2->b_nextbucket;
			releaseBucket(b2->b_num, 0);
		}
	}

	return count;
}

int Bucket::searchForHashes(struct bucket *b1, segment *s,
		unsigned int pba[],
		unsigned int bnos[], unsigned int refs[]) {
	struct bucket *b2; //, *b3;
	unsigned int k, bno, bno2;
	unsigned int count = 0;

	// Remember the original bucket pointer
	bno = b1->b_num;

	// Count the already found entries
	for (k=0; k<s->seg_size; k++)
		if (pba[k] != 0) count++;

	// Search with in the current bucket b1
	for (k=0; k<s->seg_size; k++) {
		if ((pba[k] == 0) && (searchForHashBKT(b1, s->seg_hash[k],
			&pba[k], &refs[k]) == 1)) {
			count++;
			bnos[k] = b1->b_num;
		}
	}

	// If all found, return
	if (s->seg_size == count) return count;

	// If all are not found search the entire chain of buckets      
	bno2 = getFirstBucket(b1);

	while ((bno2 != 0) && (count < s->seg_size)) {
		if (bno2 == bno) // current bucket?
			bno2 = b1->b_nextbucket;
		else {
			b2 = getBucket(bno2);
			for (k=0; k<s->seg_size; k++) {

				if ((pba[k] == 0) && (searchForHashBKT(b2,
					s->seg_hash[k], &pba[k], &refs[k]) == 1)) {
					count++;
					bnos[k] = b2->b_num;
				}
			}
			bno2 = b2->b_nextbucket;
			releaseBucket(b2->b_num, 0);
		}
	}

	return count;
}


// Delete pba entry from the chain of buckets b1
int Bucket::deletePba(struct bucket *b1, unsigned int pba) {
	struct bucket *b2;
	unsigned int bno, bno2;
	int done = 0;

	// Delete from the current bucket
	if (deleteBlockBKT(b1, pba) == 1) {
		if (b1->b_nent == 0) // If possible, delete the bucket
			deleteBKT(b1);
		return 1;
	}

	// If not found, search the entire chain of buckets
	bno2 = getFirstBucket(b1);
	bno = b1->b_num;

	while ((bno2 != 0) && (!done)){
		if (bno2 == bno) // current bucket?
			bno2 = b1->b_nextbucket;
		else {
			b2 = getBucket(bno2);

			done = deleteBlockBKT(b2, pba);
			if (done && (b2->b_nent == 0))
				deleteBKT(b2);

			bno2 = b2->b_nextbucket;
			releaseBucket(b2->b_num, 0);
		}
	}

	return done;
}

unsigned int Bucket::searchForPba(struct bucket *b, unsigned int pba,
		unsigned char hash[], unsigned int *vbno, 
		unsigned int *refcount) {
	unsigned int done = 0;
	unsigned int bno, bno2;
	struct bucket *b2;

	*vbno = 0;

	// Search in the current bucket
	if (searchForPbaBKT(b, pba, hash, refcount) == 1) {
		*vbno = b->b_num;
		return 1;
	}

	// If not found, search the entire chain of buckets
	bno2 = getFirstBucket(b);
	bno = b->b_num;

	while ((bno2 != 0) && (!done)){
		if (bno2 == bno) // current bucket?
			bno2 = b->b_nextbucket;
		else {
			b2 = getBucket(bno2);

			done = searchForPbaBKT(b2, pba, hash, refcount);
			if (done) *vbno = bno2;

			bno2 = b2->b_nextbucket;
			releaseBucket(b2->b_num, 0);
		}
	}

	return done;
}

unsigned int Bucket::searchForHash(struct bucket *b, unsigned char hash[], 
		unsigned int *pba, unsigned int *vbno, unsigned int *refcount) {
	unsigned int done = 0;
	unsigned int bno, bno2;
	struct bucket *b2;

	*vbno = 0;

	// Search in the current bucket
	if (searchForHashBKT(b, hash, pba, refcount) == 1) {
		*vbno = b->b_num;
		return 1;
	}

	// If not found, search the entire chain of buckets
	bno2 = getFirstBucket(b);
	bno = b->b_num;

	while ((bno2 != 0) && (!done)){
		if (bno2 == bno) // current bucket?
			bno2 = b->b_nextbucket;
		else {
			b2 = getBucket(bno2);

			done = searchForHashBKT(b2, hash, pba, refcount);
			if (done) *vbno = bno2;

			bno2 = b2->b_nextbucket;
			releaseBucket(b2->b_num, 0);
		}
	}

	return done;
}


pbaList * Bucket::insertValWrap(unsigned int bno, unsigned int pba, 
		unsigned char hash[16], unsigned int refcount) {
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
	else bvdelete(c);	// Delete from the old position

	pl1 = bvinsertval(c, pba, hash, refcount, CACHE_ENTRY_DIRTY);
	bvprepend(c);

	return pl1;
}


pbaList * Bucket::searchHashWrap(unsigned int bno, unsigned char hash[], 
		unsigned int *pba, unsigned int *ref) {
	struct BKTVal *c; 
	pbaList *pl; 
	struct bucket * b;
	unsigned int vbno; 

	*pba = 0;
	*ref = 0;

	BKTSEARCH++;

	// Search for the bucket entry in the cache
	c = bvsearch(bno);
	if (c != NULL) {
		pl = plsearchh(c, hash);
		
		if (pl != NULL) {
			// Entry found in cache
			*pba = pl->pba;
			*ref = pl->refcount;
			bvdelete(c);
			bvprepend(c);
			return pl;
		}
	}
		
	// Entry not found, so search the bucket
	b = getBucket(bno-HASH_BUCK_COUNT);

	searchForHash(b, hash, pba, &vbno, ref);
	releaseBucket(bno-HASH_BUCK_COUNT, 0);
	if (*pba != 0) { 
		// Entry found in the bucket
		// Create a cached entry
		pl = insertValWrap(vbno+HASH_BUCK_COUNT, *pba, hash, *ref); 
		pl->flags = CACHE_ENTRY_CLEAN | CACHE_ENTRY_COPY;
		return pl;
	}

	// Entry does not exist even in the bucket
	return NULL;
}

int Bucket::getRefCountWrap(unsigned int bno, unsigned int pba) {
	struct BKTVal *c; 
	pbaList *pl; 
	unsigned int vbno, ref;
	int refcount = 0;
	unsigned char hash[16];
	struct bucket *b;

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
	b = getBucket(bno-HASH_BUCK_COUNT);
	searchForPba(b, pba, hash, &vbno, &ref);
	releaseBucket(bno-HASH_BUCK_COUNT, 0);

	if (vbno != 0) { 
		// Entry found in the bucket
		// Create a cached entry, if necessary
		refcount = (int) ref;
		pl = insertValWrap(vbno+HASH_BUCK_COUNT, pba, hash, ref); 
		pl->flags = CACHE_ENTRY_CLEAN | CACHE_ENTRY_COPY;
	}

	return refcount;
}

int Bucket::decrementRefValWrap(unsigned int bno, unsigned int pba) {
	struct BKTVal *c; 
	pbaList *pl; 
	unsigned int vbno, ref;
	int refcount;
	unsigned char hash[16];
	struct bucket *b;

	BKTSEARCH++;
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
					//assert(readBucket(bno-HASH_BUCK_COUNT, &b) == 0);
					b = getBucket(bno-HASH_BUCK_COUNT);

					deletePba(b, pba);

					releaseBucket(bno-HASH_BUCK_COUNT, CACHE_ENTRY_DIRTY);
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
	//assert(readBucket(bno-HASH_BUCK_COUNT, &b) == 0);
	b = getBucket(bno-HASH_BUCK_COUNT);
	searchForPba(b, pba, hash, &vbno, &ref);
	releaseBucket(bno-HASH_BUCK_COUNT, 0);
	if (vbno != 0) { 
		// Entry found in the bucket
		// Create a cached entry, if necessary
		--ref;
		refcount = (int) ref;
		if (refcount > 0) {
			pl = insertValWrap(vbno+HASH_BUCK_COUNT, pba, hash, refcount); 
			pl->flags = CACHE_ENTRY_DIRTY | CACHE_ENTRY_COPY;
		}
		else {
			if (vbno + HASH_BUCK_COUNT != bno) {
				b = getBucket(vbno);
			}
			else b = getBucket(bno-HASH_BUCK_COUNT);
			deletePba(b, pba);
			releaseBucket(b->b_num, CACHE_ENTRY_DIRTY);
		}
	}
	else {
		refcount = -1;
		cout << "Error: entry not found 5\n";
		assert (refcount != -1);
		exit(0);
	}

	return refcount;
}

int Bucket::incrementRefValWrap(unsigned int bno, unsigned int pba) {
	struct BKTVal *c; 
	pbaList *pl; 
	unsigned int vbno, ref;
	int refcount = 0;
	unsigned char hash[16];
	struct bucket *b;

	BKTSEARCH++;
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
	b = getBucket(bno - HASH_BUCK_COUNT);
	searchForPba(b, pba, hash, &vbno, &ref);
	releaseBucket(bno-HASH_BUCK_COUNT, 0);
	if (vbno != 0) { 
		// Entry found in the bucket
		// Create a cached entry, if necessary
		++ref;
		refcount = (int) ref;
		pl = insertValWrap(vbno+HASH_BUCK_COUNT, pba, hash, refcount);
		pl->flags = CACHE_ENTRY_DIRTY | CACHE_ENTRY_COPY;
	}
	else {
		refcount = -1;
		cout << "Error: entry not found 6\n";
		exit(0);
	}
	return refcount;
}

int Bucket::updateValWrap(unsigned int bno, unsigned int pba, unsigned char hash[], unsigned int ref) {
	struct BKTVal *c; 
	pbaList *pl; 
	unsigned int vbno, oref;
	int refcount = 0;
	unsigned char ohash[16];
	struct bucket *b;

	BKTUPDATE++;
	c = bvsearch(bno);
	if (c != NULL) {
		pl = plsearchp(c, pba);

		if (pl != NULL) {
			// Entry found in cache
			pl->pba = pba;
			memcpy(pl->fp, hash, 16);
			pl->refcount = ref;
			pl->flags |= CACHE_ENTRY_DIRTY;
			bvdelete(c);
			bvprepend(c);
			return ref;
		}
	}

	// Entry not found, so search the bucket
	b = getBucket(bno - HASH_BUCK_COUNT);
	searchForPba(b, pba, ohash, &vbno, &oref);
	releaseBucket(bno-HASH_BUCK_COUNT, 0);
	if (vbno != 0) { 
		// Entry found in the bucket
		// Create a cached entry, if necessary
		pl = insertValWrap(vbno+HASH_BUCK_COUNT, pba, hash, ref);
		pl->flags = CACHE_ENTRY_DIRTY | CACHE_ENTRY_COPY;
		refcount = ref;
	}
	else {
		refcount = -1;
		cout << "Error: entry not found 6\n";
		exit(0);
	}
	return refcount;
}



int Bucket::searchHashesWrap(unsigned int bno, WriteReqBuf *w, 
		unsigned int seg_size, unsigned int pba[], 
		unsigned int bnos[], unsigned int refcount[]) {
	unsigned int i;
	struct BKTVal *c; 
	pbaList *pl; 
	struct bucket *b;
	unsigned int count = 0;
	WriteReqBuf *t;
	unsigned int lbnos[MAX_DEDUP_SEG_SIZE];

	BKTSEARCH += seg_size;

	// Search for the bucket entry in the cache
	c = bvsearch(bno);
	if (c != NULL) {
		pl = c->val;
		while ((pl != NULL) && (count < seg_size)){
			t = w;
			for (i=0; (i<seg_size) && (count<seg_size); i++) {
				if ((pba[i] == 0) && (memcmp(pl->fp, t->wr_md5hash, 16) == 0)) {
					// Matching entry found in cache
					//BNO2PBA_CACHE_HIT++;
					pba[i] = pl->pba;
					bnos[i] = bno;
					refcount[i] = pl->refcount;
					count++;
					break;
				}
				t = t->wr_lbanext;
			}

			pl = pl->next;
		}
		bvdelete(c);
		bvprepend(c);
	}

	// All entries not found, so search the bucket
	if (count < seg_size) {
		for (i=0; i<seg_size; i++)
			lbnos[i] = bnos[i];

		b = getBucket(bno - HASH_BUCK_COUNT);
		count = searchForHashes(b, w, seg_size, pba, lbnos, refcount);
		releaseBucket(bno - HASH_BUCK_COUNT, 0);
		for (i=0; i<seg_size; i++)
			if ((bnos[i] == 0) && (lbnos[i] != 0))
				bnos[i] = lbnos[i] + HASH_BUCK_COUNT;
	}

	return count;
}

int Bucket::searchHashesWrap(unsigned int bno, HashList *w, 
		unsigned int seg_size, unsigned int pba[], 
		unsigned int bnos[], unsigned int refcount[]) {
	unsigned int i;
	struct BKTVal *c; 
	pbaList *pl; 
	struct bucket *b;
	unsigned int count = 0;
	HashList *t;
	unsigned int lbnos[MAX_DEDUP_SEG_SIZE];

	BKTSEARCH += seg_size;

	// Search for the bucket entry in the cache
	c = bvsearch(bno);
	if (c != NULL) {
		pl = c->val;
		while ((pl != NULL) && (count < seg_size)){
			t = w;
			for (i=0; (i<seg_size) && (count<seg_size); i++) {
				if ((pba[i] == 0) && (memcmp(pl->fp, t->hl_hash, 16) == 0)) {
					// Matching entry found in cache
					//BNO2PBA_CACHE_HIT++;
					pba[i] = pl->pba;
					bnos[i] = bno;
					refcount[i] = pl->refcount;
					count++;
					break;
				}
				t = t->hl_next;
			}

			pl = pl->next;
		}
		bvdelete(c);
		bvprepend(c);
	}

	// All entries not found, so search the bucket
	if (count < seg_size) {
		for (i=0; i<seg_size; i++)
			lbnos[i] = bnos[i];

		b = getBucket(bno - HASH_BUCK_COUNT);
		count = searchForHashes(b, w, seg_size, pba, lbnos, refcount);
		releaseBucket(bno - HASH_BUCK_COUNT, 0);
		for (i=0; i<seg_size; i++)
			if ((bnos[i] == 0) && (lbnos[i] != 0))
				bnos[i] = lbnos[i] + HASH_BUCK_COUNT;
	}

	return count;
}

int Bucket::searchHashesWrap(unsigned int bno, struct segment *s,
		unsigned int pba[],
		unsigned int bnos[], unsigned int refcount[]) {
	unsigned int i;
	struct BKTVal *c;
	pbaList *pl;
	struct bucket *b;
	unsigned int count = 0;
	unsigned int lbnos[MAX_DEDUP_SEG_SIZE];

	BKTSEARCH += s->seg_size;

	// Search for the bucket entry in the cache
	c = bvsearch(bno);
	if (c != NULL) {
		pl = c->val;
		while ((pl != NULL) && (count < s->seg_size)){
			for (i=0; (i<s->seg_size) && (count<s->seg_size); i++) {
				if ((pba[i] == 0) && (memcmp(pl->fp, s->seg_hash[i], 16) == 0)) {
					// Matching entry found in cache
					//BNO2PBA_CACHE_HIT++;
					pba[i] = pl->pba;
					bnos[i] = bno;
					refcount[i] = pl->refcount;
					count++;
					break;
				}
			}

			pl = pl->next;
		}
		bvdelete(c);
		bvprepend(c);
	}

	// All entries not found, so search the bucket
	if (count < s->seg_size) {
		for (i=0; i<s->seg_size; i++)
			lbnos[i] = bnos[i];

		b = getBucket(bno - HASH_BUCK_COUNT);
		count = searchForHashes(b, s, pba, lbnos, refcount);
		releaseBucket(bno - HASH_BUCK_COUNT, 0);
		for (i=0; i<s->seg_size; i++)
			if ((bnos[i] == 0) && (lbnos[i] != 0))
				bnos[i] = lbnos[i] + HASH_BUCK_COUNT;
	}

	return count;
}


void Bucket::flushValWrap(void) {
	struct BKTVal *c; 

	while (KVTail != NULL) {
		c = KVTail;

		bvflush(c);
		bvdelete(c);

		c->next = FL;
		FL = c;
	}
}



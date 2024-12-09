#ifndef __HBUCKET_H__
#define __HBUCKET_H__

using namespace std;

#include "freelist.h"

#include "buckcomn.h"
#include "lru.h"
#include "darc.h"

#define BLKS_PER_HBKT	20	// Hash bucket entry count

struct hashbucket {
	unsigned int    hb_num;
	unsigned int    hb_nent;
	unsigned int    hb_nextbucket;
	unsigned int    hb_prevbucket;
	unsigned char   hb_fp[BLKS_PER_HBKT][16];
	unsigned int    hb_pba[BLKS_PER_HBKT];
	unsigned int    hb_pbaref[BLKS_PER_HBKT];
	char		hb_padding[16];
};

class StorageNode;
class HashBucket {
private:
#ifdef	DARCP
	DARC *bc; //(lbaIndex, 32*1024*1024); // Hash Bucket cache
#else
	LRU *bc; //(lbaIndex, 32*1024*1024); // Hash Bucket cache
#endif
	fstream bfs;	// Hash Buckets database (array of buckets)
	
	unsigned int maxbuckets; // = 64*1024*1024;
	unsigned int incr;
	
	FreeList 	*flist;
	unsigned int	nextFreeHBucket;	

	//Key Value cache
	struct BKTVal	*FL;
	unsigned int	KVCacheSize;
	unsigned int	KVHASH;
	struct BKTVal	**KVQ;
	struct BKTVal	*KVHead;
	struct BKTVal	*KVTail;

	struct pbaList	*PFL;
	unsigned int	plCacheSize;
	StorageNode	*b_sn;

	// Find a free bucket
	unsigned int findFreeHBucket(); // unsigned int start);

	unsigned int isPbaListEmpty(unsigned int bno);

	struct hashbucket * getBucket(unsigned int n);
        int putBucket(unsigned int n, struct hashbucket *b);
        void releaseBucket(unsigned int n, unsigned short dirty);

	unsigned int getFirstBucket(struct hashbucket *b);

	// Add one block to the bucket b
	unsigned int addBlockBKT(struct hashbucket *b, 
			unsigned char *hash, unsigned int pba, 
			unsigned int refcount);

	// Delete one block from bucket b
	unsigned int deleteBlockBKT(struct hashbucket *b, 
			unsigned int pba);

	// Delete bucket b from chain of buckets
	unsigned int deleteBKT(struct hashbucket *b);

	// Update one block in bucket b
	unsigned int updateBlockBKT(struct hashbucket *b, 
			unsigned char *hash, unsigned int pba, 
			unsigned int refcount);

	// Update list of blocks l, in bucket b
	// Retuns completed list in comp, remaining list in l
	unsigned int updateBlocksBKT(struct hashbucket *b, 
			struct pbaList **l, struct pbaList **comp);

	// Search for one block in bucket b
	unsigned int searchForPbaBKT(struct hashbucket *b, 
			unsigned int pba, unsigned char hash[], 
			unsigned int *refcount);

	// Search for one hash in bucket b
	unsigned int searchForHashBKT(struct hashbucket *b, 
			unsigned char hash[], unsigned int *pba, 
			unsigned int *refcount);

	pbaList * plalloc(void);
	pbaList * plsearchp(BKTVal *b, unsigned int pba);
	pbaList * plsearchh(BKTVal *b, unsigned char *hash);
	void pldelete(BKTVal *b, pbaList *p);
	BKTVal * bvalloc(void);
	void bvdelete(BKTVal *b);
	void bvprepend(BKTVal *b);
	void bvflush(BKTVal *bv);
	BKTVal * bvsearch(unsigned int bno);
	pbaList * bvinsertval(BKTVal *bv, unsigned int pba, unsigned char *hash, unsigned int refcount, unsigned short flg);
public:
	unsigned long long int HBUCKET_READ_COUNT;
	unsigned long long int HBUCKET_READ_CACHEHIT;
	unsigned long long int HBUCKET_WRITE_COUNT;
	unsigned long long int HBUCKET_WRITE_CACHEHIT;
	unsigned long long int HBUCKET_USED_COUNT;
	unsigned long long int HBUCKET_EFFECTIVE_WRITES;
	unsigned long long int HBUCKET_FLUSH_WRITES;

	unsigned long long int HBUCKET_READ_COUNT_READ;
	unsigned long long int HBUCKET_WRITE_COUNT_READ;
	unsigned long long int HBUCKET_READ_COUNT_WRITE;
	unsigned long long int HBUCKET_WRITE_COUNT_WRITE;
	unsigned long long int BKTSEARCH;
	unsigned long long int BKTINSERT;
	unsigned long long int BKTUPDATE;
	unsigned long long int BKTDELETE;

	HashBucket(const char *hbktfname, StorageNode *sn);
	void enableKeyValueCache(unsigned int plsize, unsigned int bcount);

	// Read bucket number n
	int readHBucket(unsigned int n, struct hashbucket *b);

	// Write bucket number n
	int writeHBucket(unsigned int n, struct hashbucket *b);

	// Flush the bucket cache and bucket index cache
	void flushCache(void);

	// Find the matching bucket for rid value
	unsigned int findHBucket(unsigned char * hash);

	// Add list of blocks
	unsigned int addBlocks(struct hashbucket *b, pbaList *l);
	int deletePba(unsigned int bno, unsigned int pba);

	unsigned int searchForHash(struct hashbucket *b, unsigned char * hash, unsigned int *pba, unsigned int *bno2, unsigned int *ref);
	unsigned int updateBlocks(struct hashbucket *b1, pbaList **l);
	unsigned int updateBlock(struct hashbucket *b1, unsigned char * hash, unsigned int pba, unsigned int refcount);
	unsigned int searchForPba(struct hashbucket *b1, unsigned int pba, unsigned char * hash, unsigned int *bno2, unsigned int *refcount);
	void displayStatistics(void);
	// Increment the reference count
	int incrementRefValWrap(unsigned int bno, unsigned int pba);
	int incrementRefValWrap(unsigned char *hash, unsigned int pba);
	// Decrement the reference count
	int decrementRefValWrap(unsigned int bno, unsigned int pba);
	int decrementRefValWrap(unsigned char *hash, unsigned int pba);
	int getRefCountWrap(unsigned int bno, unsigned int pba);
	int searchValWrap(unsigned char *hash, unsigned int *pba, unsigned int *ref);
	pbaList * insertValWrap(unsigned int bno, unsigned int pba, unsigned char *hash, unsigned int refcount);
	void flushValWrap(void);
	//static int evictor(fstream &fs, CacheEntry *ent, int disk);
};

#endif

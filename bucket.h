#ifndef __BUCKET_H__
#define __BUCKET_H__

using namespace std;
#include "dedupn.h"
#include "buckcomn.h"
#include "lru.h"
#include "darc.h"
#include "ibtree.h"

#include "freelist.h"
#include "writebuf.h"
#include "iotracereader.h"

#define BLKS_PER_BKT	20	// No of blocks per bucket

struct bucket {
	unsigned int	b_num;
	unsigned int 	b_rid;
	unsigned int	b_nent;
	unsigned int	b_nextbucket;	// For 0'th bucket, this is maxbkts
	unsigned int	b_prevbucket;	// For 0'th bucket, this is used count
	unsigned int	b_pba[BLKS_PER_BKT];
	unsigned char	b_fp[BLKS_PER_BKT][16];
	unsigned int	b_refcount[BLKS_PER_BKT];
	char		b_padding[10];
};

class StorageNode;
class Bucket {
private:
#ifdef	DARCP
	DARC *bc; 	//(lbaIndex, 32*1024*1024); // Bucket cache
#else
	LRU *bc; 	//(lbaIndex, 32*1024*1024); // Bucket cache
#endif
	fstream bfs;	// Buckets database (array of buckets)
	
	unsigned int maxbuckets; // = 64*1024*1024;
	unsigned int incr;
	
	FreeList 	*flist;
	unsigned int	nextFreeBucket;	


	// Key Value cache
	struct BKTVal *FL;
	unsigned int	KVCacheSize;
	unsigned int	KVHASH;
	struct BKTVal	**KVQ;
	struct BKTVal	*KVTail;
	struct BKTVal	*KVHead;

	struct pbaList	*PFL;
	unsigned int	plCacheSize;
	StorageNode 	*b_sn;


	// Find a free bucket
	unsigned int findFreeBucket(void);
	unsigned int isPbaListEmpty(unsigned int bno);
	struct bucket * getBucket(unsigned int n);
	int putBucket(unsigned int n, struct bucket *b);
	void releaseBucket(unsigned int n, unsigned short dirty);

	// Add one block to the bucket b
	unsigned int addBlockBKT(struct bucket *b, unsigned int pba, 
			unsigned char *hash, unsigned int refcount);

	// Delete one block from bucket b
	unsigned int deleteBlockBKT(struct bucket *b, unsigned int pba);

	// Delete bucket b from chain of buckets
	unsigned int deleteBKT(struct bucket *b);

	// Update one block in bucket b
	unsigned int updateBlockBKT(struct bucket *b, unsigned int pba, 
			unsigned char *hash, unsigned int refcount);

	// Update list of blocks l, in bucket b
	// Retuns completed list in comp, remaining list in l
	unsigned int updateBlocksBKT(struct bucket *b, struct pbaList **l, 
			struct pbaList **comp);

	// Search for one block in bucket b
	unsigned int searchForPbaBKT(struct bucket *b, unsigned int pba, 
			unsigned char hash[], unsigned int *refcount);

	// Search for one hash in bucket b
	unsigned int searchForHashBKT(struct bucket *b, unsigned char hash[], 
			unsigned int *pba, unsigned int *refcount);
	
	// Get first bucket in the chain of buckets
	unsigned int getFirstBucket(struct bucket *b);	
	
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
	IBTree *bi; //("bucketidx.dat", 64*1024*1024);
			// Bucket index btree
	unsigned long long int BUCKET_READ_COUNT;
	unsigned long long int BUCKET_READ_CACHEHIT;
	unsigned long long int BUCKET_WRITE_COUNT;
	unsigned long long int BUCKET_WRITE_CACHEHIT;
	unsigned long long int BUCKET_USED_COUNT;
	unsigned long long int BUCKET_EFFECTIVE_WRITES;
	unsigned long long int BUCKET_FLUSH_WRITES;

	unsigned long long int BUCKET_READ_COUNT_READ;
	unsigned long long int BUCKET_WRITE_COUNT_READ;
	unsigned long long int BUCKET_READ_COUNT_WRITE;
	unsigned long long int BUCKET_WRITE_COUNT_WRITE;
	unsigned long long int BKTSEARCH;
	unsigned long long int BKTINSERT;
	unsigned long long int BKTUPDATE;
	unsigned long long int BKTDELETE;

	Bucket(const char *bktfname, const char *bktidxfname, unsigned int btmax, unsigned int bthash, unsigned int incr, StorageNode *sn);

	void enableKeyValueCache(unsigned int plsize, unsigned int bcount);

	// Flush the bucket cache and bucket index cache
	void flushCache(void);

	// Find the matching bucket for rid value
	unsigned int findBucket(unsigned char * rid);

	// Read bucket number n
	int readBucket(unsigned int n, struct bucket *b);

	// Write bucket number n
	int writeBucket(unsigned int n, struct bucket *b);

	// Add one block 
	unsigned int addBlocks(struct bucket *b, pbaList *l);
	unsigned int updateBlocks(struct bucket *b, pbaList **l);
	unsigned int addNewBucket(unsigned char * rid);
	unsigned int searchForHash(struct bucket *b, unsigned char * h, unsigned int *pba, unsigned int *bno2, unsigned int *refcount);

	int searchForHashes(struct bucket *b, struct segment *s, unsigned int pba[], unsigned int bnos[], unsigned int refcounts[]);
	int searchForHashes(struct bucket *b1, WriteReqBuf *w, unsigned int seg_size, unsigned int pba[], unsigned int bnos[], unsigned int refcounts[]);
	int searchForHashes(struct bucket *b1, HashList *w, unsigned int seg_size, unsigned int pba[], unsigned int bnos[], unsigned int refcounts[]);
	unsigned int searchForPba(struct bucket *b, unsigned int pba, unsigned char hash[], unsigned int *vbno, unsigned int *refcount);
	int deletePba(struct bucket *b, unsigned int pba);
	void displayStatistics(void);

	// Wrapper routines
	void flushValWrap(void);
	int searchHashesWrap(unsigned int bno, WriteReqBuf *w,
                unsigned int seg_size, unsigned int pba[],
                unsigned int bnos[], unsigned int refcount[]);
	int searchHashesWrap(unsigned int bno, segment *s,
                unsigned int pba[],
                unsigned int bnos[], unsigned int refcount[]);
	int searchHashesWrap(unsigned int bno, HashList *w,
		unsigned int seg_size, unsigned int pba[],
		unsigned int bnos[], unsigned int refcount[]);
	pbaList * searchHashWrap(unsigned int bno, unsigned char hash[],
                unsigned int *pba, unsigned int *ref);
	int incrementRefValWrap(unsigned int bno, unsigned int pba);
	int decrementRefValWrap(unsigned int bno, unsigned int pba);
	int getRefCountWrap(unsigned int bno, unsigned int pba);
	pbaList * insertValWrap(unsigned int bno, unsigned int pba,
                unsigned char hash[16], unsigned int refcount);
	unsigned int getFirstBucketWrap(unsigned int bno);
	int updateValWrap(unsigned int bno, unsigned int pba,
                unsigned char hash[16], unsigned int refcount);
};

#endif





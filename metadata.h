#ifndef	__METADATA_H__
#define	__METADATA_H__	1

#include <fstream>

using namespace std;
//#include "dedupconfig.h"
//#include "dedupn.h"
#include "ibtree.h"
#include "hbtree.h"
#include "fhbtree.h"
#include "fblist.h"
#include "bucket.h"
#include "hbucket.h"
#include "pbaref.h"

class StorageNode;

class MDMaps {
public:
	IBTree		*bt;	// Lba btree (lba->pba map)
	IBTree		*pb;	// Pba btree (pba->bucket map)
	HASHBTree	*hbt;	// Hash Btree (hash->pba map)
	FileHASHBTree	*fhbt;	// Whole file hash btree
	Bucket		*bkt;	// Bucket (similarity based bucket)
	HashBucket	*hbkt;	// Hash Bucket (hash data structure)
	PbaRefOps	*pbaref;// Pba reference counts
	FBList		*fbl;	// File blocks list (counting method)
	unsigned long long int 	metaDataSize;

	MDMaps(const char *lbamapname, const char *pbamapname,
		const char *hbtmapname, const char *fhbtmapname,
		const char *bktname, const char *bktidxname, 
		const char *hbktname, const char *pbarefname, 
		const char *fblname, int dedup, StorageNode *sn);
	void flushCache(void);
	void displayStatistics(void);
	~MDMaps(void);
};

#endif


#ifndef __PBAREF_H__
#define __PBAREF_H__

using namespace std;

class MDMaps;

struct PbaRef {
	unsigned char 	pr_hash[16];
	unsigned int	pr_ref;
};

struct PbaRefIncore {
	unsigned int 	key;
	unsigned char 	hash[16];
	unsigned int	ref;
	struct PbaRefIncore	*next, *prev;
	struct PbaRefIncore	*tnext, *tprev;
	unsigned short	flags;
};

class StorageNode;
class PbaRefOps {
	fstream pref;
	char prefname[256];
	struct PbaRefIncore	*FL;
	unsigned int		KVCacheSize;
	unsigned int		KVHASH;
	struct PbaRefIncore	**KVQ;
	struct PbaRefIncore	*KVTail;
	struct PbaRefIncore	*KVHead;
	unsigned long int	maxb;
	StorageNode 		*psn;

	void pbaRead(unsigned int pba, PbaRefIncore *p);
	void pbaWrite(PbaRefIncore *p);
	void pbaRefZero(PbaRefIncore *p);
	void checkSeekPos(unsigned int pba);
	PbaRefIncore * kvalloc(void);
	PbaRefIncore * kvsearch(unsigned int i, unsigned int key);
	void kvdelete(unsigned int i, PbaRefIncore *c);
	void kvprepend(unsigned int i, PbaRefIncore *c);
	unsigned int kvflush(void);

public:
	//=====================================
	unsigned long long int PBAREF_CACHE_HIT;
	unsigned long long int PBAREF_CACHE_MISS;
	unsigned long long int PBREFSEARCH;
	unsigned long long int PBREFINSERT;
	unsigned long long int PBREFDELETE;
	unsigned long long int PBREFUPDATE;

	unsigned long long int PBREF_READCOUNT_READ;
	unsigned long long int PBREF_WRITECOUNT_READ;
	unsigned long long int PBREF_READCOUNT_WRITE;
	unsigned long long int PBREF_WRITECOUNT_WRITE;
	//=====================================

	PbaRefOps(StorageNode *sn);
	PbaRefOps(const char *pbarefname, unsigned int csize, StorageNode *sn);
	void pbaRefCacheInit(unsigned int size);
	unsigned int incrementPbaRef(unsigned int pba, unsigned char *hash);
	unsigned int decrementPbaRef(unsigned int pba);
	unsigned int flushCache(void);
};

#endif

#ifndef	__FBLIST_H__
#define	__FBLIST_H__	1

using namespace std;

//#include "dedupconfig.h"
//#include "dedupn.h"

struct FBEntry {
	unsigned int fbe_pba;	// segment PBA/LBA number ( = 0, for free entry)
	unsigned int fbe_snode;	// Storage node, on which this segment 
				// is present
	unsigned int fbe_nb;	// Number of blocks
	unsigned int fbe_next;	// Next entry, slot number
};

struct FBEntryIncore {
	unsigned int 		key;	// Entry number
	unsigned int 		pba;	
	unsigned int 		snode;
	unsigned int 		nb;
	unsigned int 		enext;
	struct FBEntryIncore 	*next, *prev;
	struct FBEntryIncore 	*tnext, *tprev;
	unsigned short 		flags;
};

class FBList {
	fstream	fbref;
	char 	fbname[256];

	struct FBEntryIncore	*FL;
	unsigned int		KVCacheSize;
	unsigned int		KVHASH;
	struct FBEntryIncore	**KVQ;
	struct FBEntryIncore	*KVTail;
	struct FBEntryIncore	*KVHead;
	unsigned long int	maxb;

	void fbeRead(unsigned int n, FBEntryIncore *p);
	void fbeWrite(FBEntryIncore *p);
	void checkSeekPos(unsigned int ent);
	FBEntryIncore * kvalloc(void);
	FBEntryIncore * kvsearch(unsigned int i, unsigned int key);
	void kvdelete(unsigned int i, FBEntryIncore *c);
	void kvprepend(unsigned int i, FBEntryIncore *c);
	unsigned int kvflush(void);

public:
	//=====================================
	unsigned long long int FBE_CACHE_HIT;
	unsigned long long int FBE_CACHE_MISS;
	unsigned long long int FBESEARCH;
	unsigned long long int FBEINSERT;
	unsigned long long int FBEDELETE;
	unsigned long long int FBEUPDATE;

	//unsigned long long int FBE_READCOUNT_READ;
	//unsigned long long int FBE_WRITECOUNT_READ;
	//unsigned long long int FBE_READCOUNT_WRITE;
	//unsigned long long int FBE_WRITECOUNT_WRITE;
	//=====================================

	FBList(void);
	FBList(const char *fbname, unsigned int csize);
	void fbeCacheInit(unsigned int size);
	void fbeGetEntry(unsigned int n, unsigned int *pba, 
			unsigned int *sn, unsigned int *nb, unsigned int *next);
	void fbePutEntry(unsigned int n, unsigned int pba, 
			unsigned int sn, unsigned int nb, unsigned int next);
	unsigned int getFreeFbe(void);
	void freeFbe(unsigned int n);
	unsigned int flushCache(void);
};

#endif


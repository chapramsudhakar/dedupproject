#ifndef __CACHEMEM_H__
#define __CACHEMEM_H__

using namespace std;

//#include "dedupconfig.h"
//#include "dedupn.h"
//#include "cachesize.h"
#include "cache.h"
#include "writebuf.h"

#define	CEMSLABSIZE	(256 * 1024)

struct mblock {
	char 			*mb_ptr;
	struct CacheEntry	*mb_flist;
	struct mblock		*mb_next;
	struct mblock		*mb_prev;
	unsigned int		mb_nactive;
};

struct b512fl {
	struct b512fl 	*next;
};

class CacheMem {
private:
	struct CacheEntry       *cem, *CEMBLOCK;
	struct WriteReqBuf      *wrbm, *WRBMBLOCK;

	struct mblock   *cemslabs;

	unsigned int    cemcount; // = 0;
	unsigned int    b512mcount; // = 0;
	unsigned int    wrbmcount; // = 0;
	unsigned int    CEMSLABCOUNT; // = 0;

	char    *b512mblocks;
	struct b512fl   *fl;

	long long int alloccount, freecount;

	void cegb(void);
public:
	char cachememname[50];
	void initCacheMem(const char *name);
	void createCacheEntries(void);
	WriteReqBuf *wrbmmalloc(void);
	void wrbmfree(WriteReqBuf *c);

	CacheEntry *cemalloc();
	void cefree(CacheEntry *c);

	char *b512malloc();
	void b512free(char *c);
	void cacheMemStats(void);
};

#endif

#ifndef __CACHE_H__
#define __CACHE_H__

using namespace std;

#define CACHE_ENTRY_BUSY	0x0008
#define CACHE_ENTRY_UPPER	0x0004
#define CACHE_ENTRY_COPY	0x0002
#define CACHE_ENTRY_DIRTY	0x0001
#define CACHE_ENTRY_CLEAN	0x0000

struct CacheEntry {
	unsigned long long int 	ce_rtime;	// Last access time
	unsigned long long int	ce_wtime;	// Last modification time
	unsigned int		ce_key;		// Key value
	void			*ce_object;	// Object pointer to cached item
	unsigned int		ce_wrefcount;	// Write Reference count
	unsigned int		ce_refcount;	// Total Reference count
	unsigned short		ce_size;	// Size of the info
	unsigned short		ce_flags;	// State info
	unsigned short		ce_lckcnt;
	CacheEntry		*ce_next;	// Hash list ptrs
	CacheEntry		*ce_prev;
	CacheEntry		*ce_tnext;	// Time wise list ptrs
	CacheEntry		*ce_tprev;
};


CacheEntry * createCacheEntry(unsigned int key, void *obj, 
		unsigned short size, unsigned long long int rtime, 
		unsigned long long int wtime, unsigned short flags);

void lockEntry(CacheEntry *c);
void unlockEntry(CacheEntry *c);

#endif



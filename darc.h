#ifndef __DARC_H__
#define __DARC_H__

using namespace std;

#include "cache.h"
#include "lrulist.h"

class StorageNode;

class DARC {
private:
	char			c_name[30]; // Cache name
	unsigned int 		c_count;	// Total count of entries
	unsigned long		c_size;		// Total size of the cache
	unsigned long		CACHE_REPLACEMENTS;
	
	LRUList *T1;	// Most recently used single time referred LRU list
	LRUList *T2;	// Most frequently used LRU list
	LRUList *B1;	// Meta data only single time referred LRU list or
			// To be evicted list
	LRUList *B2;	// Meta data only frequently used LRU list or
			// To be evicted list
	int p;	// Desired size of T1
	StorageNode	*c_sn;
	CacheEntry * IP;	// Insertion point in T2
	unsigned long int below;// Count of entries below IP;

	void movetoT2(CacheEntry *c);
	CacheEntry *replace(unsigned int p, int found);
	float popularity(CacheEntry *c);
	unsigned int evictEntry(fstream &fs, CacheEntry *c, int disk);
	void adjustIP(void);
	CacheEntry *findEntry(unsigned int *count, fstream &fs, int disk, int (* evictor)(fstream &fs, CacheEntry *ent, int dsk));
public:
	unsigned int		HASH_SIZE;
	unsigned int		c_dcount;	// Dirty count of entries
	unsigned long		c_maxsize;	// Upper limit memory size
	unsigned long		c_maxcount;	// Upper limit entry count
	unsigned int		c_objsize;	// One object size

	float 			c_rrefs;	// Total read references
	float 			c_wrefs;	// Total read references
	unsigned long int	c_maxbelow;	// Maximum no of entries below 
						// Insertion Point (IP)

#define WDUPES	0.4	// Weightage for the number of LBA duplicates
			// usable for data cache or pba cache
			// Otherwise 0
#define	WDIRTY	0.4	// Weightage for dirty reference
			// If not data cache or pba cache 0.7
#define WCLEAN	0.2	// Weightage for clean reference
			// If not data cache or pba cache 0.3
#define WMDIRTY	0.7
#define WMCLEAN	0.3

	DARC(const char *name, unsigned int hashsize, unsigned long maxsize, unsigned int objsize, StorageNode *sn);
	unsigned int flushCache(fstream &fs, unsigned int objsize, int disk);
	void disableCache(fstream &fs, unsigned int objsize, int disk);

	//void freeCache(void);

	// Adding a new cache entry with all details, intialized
	CacheEntry *addEntry(CacheEntry *c);
	// Adding a new cache entry by replaceing an existing entry
	// When the total cache size reaches the limit this will be called
	unsigned int addReplaceEntry(fstream &fs, unsigned int key, void *obj, 
			unsigned short size, unsigned long long int rtime, 
			unsigned long long int wtime, int disk, 
			unsigned short flags, 
			int (* evictor)(fstream &fs, CacheEntry *ent, int disk), CacheEntry **ent);
	// Search for the entry with key 
	CacheEntry * searchCache(unsigned int key);
	CacheEntry * lookCache(unsigned int key);

	// To select a victim entry and removing it from the list
	//CacheEntry * selectVictim(void);
	void deleteEntry(CacheEntry *c);
	void repositionEntry(CacheEntry *c);
	void newInterval(void);
	void decayWeights(void);
	void appendEntry(CacheEntry *c);
	unsigned int getCount(void);
	unsigned long getSize(void);
	void displayStatistics(void);
};

#endif

#ifndef __LRU_H__
#define __LRU_H__

using namespace std;
#include "cache.h"
//#include "storagenode.h"

class StorageNode;

class LRU {
private:
	char			c_name[30]; // Cache name
	unsigned int 		c_count;	// Total count of entries
	StorageNode		*c_sn;		// Storage node

public:
	unsigned long		c_size;		// Total size of the cache

	unsigned long		CACHE_REPLACEMENTS;
	struct CacheEntry	*c_thead;
	struct CacheEntry	*c_ttail;
	struct CacheEntry 	**c_hdrs;// Hashed entries
	struct CacheEntry	**c_tail;//Tail end pointer
	unsigned int		HASH_SIZE;
	unsigned int		c_dcount;	// Dirty count of entries
	unsigned long		c_maxsize;	// Upper limit
	float			c_rrefs;	// Total read references
	float			c_wrefs;	// Total write references
	LRU(const char *name, unsigned int hashsize, unsigned long maxsize, StorageNode *sn);

	int isFull();

	void freeCache(void);	

	// Adding a new cache entry with all details
	CacheEntry * addEntry(unsigned int key, void *obj, 
			unsigned short size, unsigned long long int rtime, 
			unsigned long long int wtime, unsigned short flags);

	// Adding a new cache entry with all details
	int addReplaceEntry(fstream &fs, unsigned int key, void *obj, 
			unsigned short size, unsigned long long int rtime, 
			unsigned long long int wtime, int disk, 
			unsigned short flags, 
			int (*evictor)(fstream &fs, CacheEntry *ent, int disk),
		       	CacheEntry **ent);

	// To select a victim entry and removing it from the list
	CacheEntry * selectVictim(void);

	// Adding a new cache entry by replaceing an existing entry
	// When the total cache size reaches the limit this will be called
	unsigned int replaceEntry(fstream &fs, unsigned int key, void *obj, 
			unsigned short size, unsigned long long int rtime, 
			unsigned long long int wtime, int disk, 
			unsigned short flags,
			int (*evictor)(fstream &fs, CacheEntry *ent, int disk));

	// Disabling of the cache, first flush all the dirty blocks
	// then release all the memory occupied by the cache objects and 
	// cache entries
	void disableCache(fstream &fs, unsigned int objsize, int disk);

	// Write all dirty objects to the file stream
	unsigned int flushCache(fstream &fs, unsigned int objsize, int disk); 

	void repositionEntry(CacheEntry *c);
	// Delete a cache entry
	void deleteEntry(CacheEntry *c);

	// Update cache entry
	void updateEntry(CacheEntry *c, int op, unsigned long long int rwtime);
	
	// Prepend an entry
	void prependEntry(CacheEntry *c);

	// insert an entry
	void insertEntry(CacheEntry *near, CacheEntry *c);

	// Append an entry
	void appendEntry(CacheEntry *c);

	// Search for the entry with key 
	CacheEntry * searchCache(unsigned int key);
	CacheEntry * lookCache(unsigned int key);

	// Count of entries
	unsigned int getCount(void);

	// Total size 
	unsigned long getSize(void);

	// Display statistics
	void displayStatistics(void);

	// Display all cache entries
	void display(void);
};

CacheEntry * createCacheEntry(unsigned int key, void *obj, 
		unsigned short size, unsigned long long int rtime, 
		unsigned long long int wtime, unsigned short flags);

#endif

#ifndef __LRULIST_H__
#define __LRULIST_H__

using namespace std;

#include "cache.h"

class StorageNode;

class LRUList {
public:
	char			c_name[30]; // Cache name
	unsigned int 		c_count;	// Total count of entries
	unsigned long		c_size;		// Total size of the cache
	unsigned long		CACHE_REPLACEMENTS;

	struct CacheEntry	*c_thead;
	struct CacheEntry	*c_ttail;
	struct CacheEntry 	**c_hdrs;// Hashed entries
	struct CacheEntry	**c_tail;//Tail end pointer
	//struct CacheEntry	*c_IP;	// Insertion position marker as per popularity
	unsigned int		HASH_SIZE;
	unsigned int		c_dcount;	// Dirty count of entries
	unsigned long		c_maxsize;	// Upper limit
	StorageNode 		*c_sn;

	LRUList(const char *name, unsigned int hashsize, unsigned long maxsize, StorageNode *sn);


	// To select a victim entry and removing it from the list
	CacheEntry * selectVictim(void);

	// Disabling of the cache, first flush all the dirty blocks
	// then release all the memory occupied by the cache objects and 
	// cache entries
	void disableCache(fstream &fs, unsigned int objsize, int disk);

	// Write all dirty objects to the file stream
	unsigned int flushCache(fstream &fs, unsigned int objsize, int disk); 

	// Delete a cache entry
	void deleteEntry(CacheEntry *c);

	// Prepend an entry
	void prependEntry(CacheEntry *c);

	// Append an entry
	void appendEntry(CacheEntry *c);

	// Insert after IP
	void insertAtIP(CacheEntry *IP, CacheEntry *c);

	// Search for the entry with key 
	CacheEntry * searchCache(unsigned int key);
	CacheEntry * lookCache(unsigned int key);

	// Count of entries
	unsigned int getCount(void);

	// Count of dirty entries
	unsigned int getDCount(void);

	// Total size 
	unsigned long getSize(void);

	// Display statistics
	void displayStatistics(void);

	// Display all cache entries
	void display(void);
};

#endif

#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <stdlib.h>

#include "cachemem.h"
#include "lrulist.h"
#include "util.h"

extern unsigned long long int curTime;

// Create a cache entry with all details
CacheEntry * createCacheEntry(unsigned int key, void *obj,
	unsigned short size, unsigned long long int rtime, 
	unsigned long long int wtime, unsigned short flags, StorageNode *sn) {
	CacheEntry *c;

	c = (CacheEntry *)sn->mem->cemalloc();
	assert(c != NULL);

	// Initialize the fields
	c->ce_key = key;
	c->ce_object = obj;
	c->ce_size = size;
	c->ce_rtime = rtime;
	c->ce_wtime = wtime;
	c->ce_refcount = 0;
	c->ce_wrefcount = 0;
	c->ce_flags = flags;
	c->ce_next = NULL;
	c->ce_prev = NULL;
	c->ce_tnext = NULL;
	c->ce_tprev = NULL;

	return c;
}


LRUList::LRUList(const char *name, unsigned int hashsize, 
		unsigned long maxsize, StorageNode *sn) {
	unsigned int i;

	strcpy(c_name, name);
	c_count = 0;
	c_dcount = 0;
	c_size = 0;
	c_maxsize = maxsize;
	c_sn = sn;
	HASH_SIZE = hashsize;
	CACHE_REPLACEMENTS = 0;
	c_hdrs = (CacheEntry **)malloc(HASH_SIZE * sizeof(CacheEntry *));	
	c_tail = (CacheEntry **)malloc(HASH_SIZE * sizeof(CacheEntry *));	
	assert((c_hdrs != NULL) && (c_tail != NULL));
	for (i=0; i<HASH_SIZE; i++) {
		c_hdrs[i] = NULL;
		c_tail[i] = NULL;
	}		
	c_thead = NULL;
	c_ttail = NULL;
}

unsigned int LRUList::flushCache(fstream &fs, unsigned int objsize, int disk) {
	CacheEntry *c;
	int count = 0;
	int wcount, wstart;

	// Write all dirty blocks
	c = c_ttail;

	while (c != NULL) {
		if (c->ce_flags & CACHE_ENTRY_DIRTY) {
			if (objsize > 0) {
				fs.seekp((unsigned long)(c->ce_key-1)*objsize);
				fs.write((char *)c->ce_object, c->ce_size);
			}

			// Clear the dirty flag
			c->ce_flags = c->ce_flags ^ CACHE_ENTRY_DIRTY;
			c_dcount--;

			if (disk >= 0) {
				wcount = c->ce_size / 512;
				wstart = (c->ce_key) * wcount;
				
				DiskWrite(c_sn, curTime, disk, wstart, wcount);
				curTime++;
				count += wcount;
			}
			else count++;
		}

		c = c->ce_tprev;
	}

	return count;
}

void LRUList::disableCache(fstream &fs, unsigned int objsize, int disk) {
	CacheEntry *c, *c1;

	// Write all dirty blocks
	cout << "Flushing cache ...\n";
	//if (objsize > 0)	// really need to be written to file?
		flushCache(fs, objsize, disk);
	cout << "Flushing cache completed\n";

	// Release all the memory occupied by the cache 
	c = c_thead;

	while (c != NULL) {
		if (c->ce_object != NULL) {
			if (c->ce_size == 512)
				c_sn->mem->b512free((char *)c->ce_object);
			else {
				cout << "Error : lrulist object size : " 
					<< c->ce_size << " is not freed\n";
			}
		}

		c1 = c->ce_tnext;
		c_sn->mem->cefree(c);
		c = c1;
	}

	c_thead = c_ttail = NULL;
	free(c_hdrs);
	free(c_tail);
	c_hdrs = c_tail = NULL;
	cout << "Cached memory is released\n";
}

CacheEntry * LRUList::selectVictim() {
	CacheEntry *c1; 

	// Select the victim entry and delete it
	c1 = c_ttail;

	// If the lckcnt > 0, it is under use/busy 
	while ((c1 != NULL) && (c1->ce_lckcnt != 0)) {
		c1 = c1->ce_tprev;
	}
	// One must have been found
	// Delete that entry
	if (c1 != NULL)
		deleteEntry(c1);

	return c1;
}

// Delete a cache entry
void LRUList::deleteEntry(CacheEntry *c) {
	int i;

	i = c->ce_key % HASH_SIZE;

	// Remove from hash queue
	if (c->ce_next != NULL)
		(c->ce_next)->ce_prev = c->ce_prev;
	else c_tail[i] = c->ce_prev;

	if (c->ce_prev != NULL)
		(c->ce_prev)->ce_next = c->ce_next;
	else c_hdrs[i] = c->ce_next;

	// Remove from time queue
	if (c->ce_tnext != NULL)
		(c->ce_tnext)->ce_tprev = c->ce_tprev;
	else c_ttail = c->ce_tprev;

	if (c->ce_tprev != NULL)
		(c->ce_tprev)->ce_tnext = c->ce_tnext;
	else c_thead = c->ce_tnext;

	
	// Reduce the total size of the cache
	c_size -= (c->ce_size); 
	c_count--;

	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount--;
}

void LRUList::insertAtIP(CacheEntry *IP, CacheEntry *c) {
	int i;

	// hash queue index 
	i = c->ce_key % HASH_SIZE;

	// Add at the front
	c->ce_next = c_hdrs[i];
	c->ce_prev = NULL;
	if (c_hdrs[i] != NULL)
		c_hdrs[i]->ce_prev = c;
	c_hdrs[i] = c;
	if (c_tail[i] == NULL) c_tail[i] = c;

	// Timewise after c_IP
	if (IP != NULL) {
		c->ce_tprev = IP;
		c->ce_tnext = IP->ce_tnext;
		if (IP->ce_tnext != NULL)
			(IP->ce_tnext)->ce_tprev = c;
		else {
			c_ttail = c;
		}
		IP->ce_tnext = c;
	}
	else {
		// Append
		c->ce_tnext = NULL;
		c->ce_tprev = c_ttail;

		if (c_ttail != NULL) {
			c_ttail->ce_tnext = c;
			c_ttail = c;
		}
		else {
			c_ttail = c_thead = c;
		}
	}

	// Add object size to total size
	c_size += (c->ce_size); 
	c_count++;

	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount++;
}

// Prepend the cache entry at the front of the list
void LRUList::prependEntry(CacheEntry *c) {
	int i;

	// hash queue index 
	i = c->ce_key % HASH_SIZE;

	// Add at the front
	c->ce_next = c_hdrs[i];
	c->ce_prev = NULL;
	if (c_hdrs[i] != NULL)
		c_hdrs[i]->ce_prev = c;
	c_hdrs[i] = c;
	if (c_tail[i] == NULL) c_tail[i] = c;

	// Timewise at the front
	c->ce_tprev = NULL;
	c->ce_tnext = c_thead;
	if (c_thead != NULL) {
		c_thead->ce_tprev = c;
		c_thead = c;
	}
	else {
		c_ttail = c_thead = c;
	}

	// Add object size to total size
	c_size += (c->ce_size); 
	c_count++;

	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount++;
}

// Append the cache entry at the end of the list
void LRUList::appendEntry(CacheEntry *c) {
	int i;

	// hash queue index 
	i = c->ce_key % HASH_SIZE;

	// Add at the rear end
	c->ce_next = NULL;
	c->ce_prev = c_tail[i];
	if (c_tail[i] != NULL)
		c_tail[i]->ce_next = c;
	c_tail[i] = c;

	if (c_hdrs[i] == NULL) c_hdrs[i] = c;

	// Timewise at the end
	c->ce_tprev = c_ttail;
	c->ce_tnext = NULL;
	if (c_ttail != NULL) {
		c_ttail->ce_tnext = c;
		c_ttail = c;
	}
	else {
		c_ttail = c_thead = c;
	}

	// Add object size to total size
	c_size += (c->ce_size); 
	c_count++;

	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount++;
}

// Search for the entry based on unsigned int
CacheEntry * LRUList::searchCache(unsigned int key) {
	int i; 
	CacheEntry *c;
	
	// hash queue index 
	i = key % HASH_SIZE;

	c = c_hdrs[i];

	while ((c != NULL) && (c->ce_key != key)) {
		c = c->ce_next;
	}

	return c;
}

// Search for the entry based on unsigned int
CacheEntry * LRUList::lookCache(unsigned int key) {
	int i; 
	CacheEntry *c;
	
	// hash queue index 
	i = key % HASH_SIZE;

	c = c_hdrs[i];

	while ((c != NULL) && (c->ce_key != key)) {
		c = c->ce_next;
	}

	return c;
}

// Count of entries
unsigned int LRUList::getCount(void) { return c_count; }

// Count of dirty entries
unsigned int LRUList::getDCount(void) { return c_dcount; }

// Total size 
unsigned long LRUList::getSize(void) { return c_size; }

void LRUList::displayStatistics(void) {
	int count, maxl = 0, minl = INT_MAX;
	unsigned int i;
	float avgl = 0.0f;
	CacheEntry *c;

	for (i=0; i<HASH_SIZE; i++) {
		c = c_hdrs[i];
		count = 0;

		while (c != NULL) {
			c = c->ce_next;
			count++;
		}

		if (count < minl) minl = count;
		if (count > maxl) maxl = count;
		avgl += count;
	}

	avgl = avgl / HASH_SIZE;
	cout << c_name << ":\nCache total count of entries \t: " << c_count
		<< "\nDirty count \t\t\t: " << c_dcount <<"\nTotal size \t\t\t: " 
		<< c_size << "\nNo of cache replacements \t: " 
		<< CACHE_REPLACEMENTS 
		<< "\nNList \t\t\t:" << HASH_SIZE 
		<< "\nMax list length \t\t:" << maxl
		<< "\nMin list length \t\t:" << minl
		<< "\nAvg list length \t\t:" << avgl << endl;
}

void LRUList::display(void) {
	CacheEntry *c;
	unsigned int i;
	
	// hash queue index 
	for (i=0; i<HASH_SIZE; i++) {
		c = c_hdrs[i];

		cout << i << "=";
		while (c != NULL) {
			cout << "->" << c->ce_key;
			c = c->ce_next;
		}
		cout << endl;
	}
}



#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <stdlib.h>

#include "cachemem.h"
#include "lru.h"
#include "util.h"

extern unsigned long long int curTime;

LRU::LRU(const char *name, unsigned int hashsize, unsigned long maxsize,
		StorageNode *sn) {
	unsigned int i;

	strcpy(c_name, name);
	c_count = 0;
	c_dcount = 0;
	c_size = 0;
	c_maxsize = maxsize;
	c_sn = sn;
	HASH_SIZE = hashsize;
	CACHE_REPLACEMENTS = 0;
	c_rrefs = c_wrefs = 0.0;
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

int LRU::isFull() { 
	if (c_size >= c_maxsize) return 1;
	else return 0;
}

// Release all cache entries
void LRU::freeCache(void) {
	CacheEntry *c, *c1;

	// Release all the memory occupied by the cache 
	c = c_thead;

	while (c != NULL) {
		c1 = c->ce_tnext;
		deleteEntry(c);
		if (c->ce_object != NULL) {
			if (c->ce_size == 512)
				c_sn->mem->b512free((char *)c->ce_object);
			else cout << "lru: unknown sized object! Not freed\n";
		}

		c_sn->mem->cefree(c);
		c = c1;
	}
}

unsigned int LRU::flushCache(fstream &fs, unsigned int objsize, int disk) {
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

void LRU::disableCache(fstream &fs, unsigned int objsize, int disk) {
	CacheEntry *c, *c1;

	// Write all dirty blocks
	cout << c_name << " Flushing cache ...\n";
	if (objsize > 0)	// really need to be written to file?
		flushCache(fs, objsize, disk);
	cout << "Flushing cache completed\n";
	// Release all the memory occupied by the cache 
	c = c_thead;

	while (c != NULL) {
		if (c->ce_object != NULL) {
			if (c->ce_size == 512)
				c_sn->mem->b512free((char *)c->ce_object);
			else cout << "lru: unknown sized object! Not freed\n";
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

void LRU::repositionEntry(CacheEntry *c) { 
	return;
}

// Adding cache entry with all details
CacheEntry * LRU::addEntry(unsigned int key, void *obj,  
	unsigned short size, unsigned long long int rtime, unsigned long long int wtime, unsigned short flags) {
	CacheEntry *c;

	c = (CacheEntry *)c_sn->mem->cemalloc();
	assert(c != NULL);

	// Initialize the fields
	c->ce_key = key;
	c->ce_object = obj;
	c->ce_size = size;
	c->ce_rtime = rtime;
	c->ce_wtime = wtime;
	c->ce_refcount = 1;
	c->ce_lckcnt = 0;
	if (wtime > 0)
		c->ce_wrefcount = 1;
	c->ce_flags = flags;

	// Find the hash queue index and add at the front
	prependEntry(c);

	return c;
}


// Adding a new cache entry by replacing an existing entry
// When the total cache size reaches the limit this will be called
unsigned int LRU::replaceEntry(fstream &fs, unsigned int key, void *obj, 
		unsigned short size, unsigned long long int rtime, 
		unsigned long long int wtime, int disk, unsigned short flags,
		int (*evictor)(fstream &fs, CacheEntry *ent, int disk)) {
	CacheEntry *c1;
	unsigned int count = 0;
	int wcount, wstart;

	CACHE_REPLACEMENTS++;

	// Select the victim entry and delete it
	c1 = selectVictim();

	// One must have been found
	// Delete that entry
	assert(c1 != NULL);

	// If dirty write its data
	if (c1->ce_flags & CACHE_ENTRY_DIRTY) {
		
		if (evictor != NULL) {
			count = evictor(fs, c1, disk);
		}
		else {
			if (c1->ce_object != NULL) {
				fs.seekp((unsigned long)(c1->ce_key-1)*c1->ce_size);
				if (fs.fail()) {
					cout << c_name << " Error cacheReplacement seek failed key " << c1->ce_key << endl; 
					fs.clear();
				}
				fs.write((char *)c1->ce_object, c1->ce_size);
			
				if (fs.fail()) {
					cout << c_name << " Error cacheReplacement write failed key " << c1->ce_key << endl;
					fs.clear();
				}
			}

			if (disk >= 0)  {
				wcount = c1->ce_size / 512;
				wstart = (c1->ce_key) * wcount;
				DiskWrite(c_sn, curTime, disk, wstart, wcount);
				count = wcount;
			}
			else {
				count = 1;
			}
		}
	}

	// Release object memory
	if (c1->ce_object != NULL) {
		if (c1->ce_size == 512)
			c_sn->mem->b512free((char *)c1->ce_object);
		else cout << "lru: Error unknown sized object! Not freed\n";
	}

	// Initialize the fields
	c1->ce_key = key;
	c1->ce_object = obj;
	c1->ce_size = size;
	c1->ce_rtime = rtime;
	c1->ce_wtime = wtime;
	c1->ce_refcount = 1;
	c1->ce_lckcnt = 0;
	if (wtime > 0)
		c1->ce_wrefcount = 1;
	c1->ce_flags = flags;

	// Prepend the cache entry
	prependEntry(c1);

	return count;
}

int LRU::addReplaceEntry(fstream &fs, unsigned int key, void *obj, 
		unsigned short size, unsigned long long int rtime, 
		unsigned long long int wtime, int disk, 
		unsigned short flags, 
		int (*evictor)(fstream &fs, CacheEntry *ent, int disk), 
		CacheEntry **ent) {
	int count;

	if ((c_size + size) <= c_maxsize) {
		// Still capacity is remaining, so add entry
		// zero replacements
		if (ent != NULL)
			*ent = addEntry(key, obj, size, rtime, wtime, flags);
		return 0;
	}
	else {
		count = replaceEntry(fs, key, obj, size, rtime, wtime, disk, flags, evictor);
		if (ent != NULL)
			*ent = lookCache(key);
		return count;
	}
}

// Selecting LRU entry as victim
CacheEntry * LRU::selectVictim() {
	CacheEntry *c1; 

	// Select the victim entry and delete it
	c1 = c_ttail;
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
void LRU::deleteEntry(CacheEntry *c) {
	int i;

	i = c->ce_key % HASH_SIZE;

	// Remove from hash queue
	if (c->ce_next != NULL) (c->ce_next)->ce_prev = c->ce_prev;
	else c_tail[i] = c->ce_prev;

	if (c->ce_prev != NULL) (c->ce_prev)->ce_next = c->ce_next;
	else c_hdrs[i] = c->ce_next;

	// Remove from time queue
	if (c->ce_tnext != NULL) (c->ce_tnext)->ce_tprev = c->ce_tprev;
	else c_ttail = c->ce_tprev;

	if (c->ce_tprev != NULL) (c->ce_tprev)->ce_tnext = c->ce_tnext;
	else c_thead = c->ce_tnext;


	// Reduce the total size of the cache
	c_size -= (c->ce_size);
	c_count--;

	if (c->ce_flags & CACHE_ENTRY_DIRTY) c_dcount--;
}

// Update cache entry
void LRU::updateEntry(CacheEntry *c, int op, unsigned long long int rwtime) {
	c->ce_refcount++;

	deleteEntry(c);

	if (op == OP_READ) c->ce_rtime = rwtime; // Millisecs 
	else {
		c->ce_wtime = rwtime; // Millisecs 
		c->ce_flags |= CACHE_ENTRY_DIRTY;
	}

	prependEntry(c);
}

// Prepend the cache entry at the front of the list
void LRU::prependEntry(CacheEntry *c) {
	int i;

	// hash queue index 
	i = c->ce_key % HASH_SIZE;

	// Add at the front
	c->ce_prev = NULL;
	c->ce_next = c_hdrs[i];
	if (c_hdrs[i] != NULL) c_hdrs[i]->ce_prev = c;
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

// Inesert the cache entry as per the time order
void LRU::insertEntry(CacheEntry *near, CacheEntry *c) {
	int i, before;
	CacheEntry *c2;
	unsigned long long int t1, t2;

	t1 = (c->ce_rtime > c->ce_wtime) ? c->ce_rtime : c->ce_wtime;

	// hash queue index 
	i = c->ce_key % HASH_SIZE;

	// Add at the front
	c->ce_prev = NULL;
	c->ce_next = c_hdrs[i];
	if (c_hdrs[i] != NULL)
		c_hdrs[i]->ce_prev = c;
	c_hdrs[i] = c;
	if (c_tail[i] == NULL) c_tail[i] = c;

	// Timewise at the appropriate place
	if (c_thead == NULL) {
		c->ce_tprev = c->ce_tnext = NULL;
		c_ttail = c_thead = c;
	}
	else {
		if (near == NULL) c2 = c_thead;
		else c2 = near;

		t2 = (c2->ce_rtime > c2->ce_wtime) ? c2->ce_rtime : c2->ce_wtime;
		if (t2 > t1) {	
			while ((t2 > t1) && (c2->ce_tnext != NULL)) {
				c2 = c2->ce_tnext;
				t2 = (c2->ce_rtime > c2->ce_wtime) ? c2->ce_rtime : c2->ce_wtime;
			}
		
			if (t2 <= t1) before = 1;
			else before = 0;
		}
		else {
			while ((t2 <= t1) && (c2->ce_tprev != NULL)) {
				c2 = c2->ce_tprev;
				t2 = (c2->ce_rtime > c2->ce_wtime) ? c2->ce_rtime : c2->ce_wtime;
			}

			if (t2 > t1) before = 0;
			else before = 1;
		}
	
		if (before == 1) {
			// Insertion before c2
			c->ce_tprev = c2->ce_tprev;
			c->ce_tnext = c2;
			if (c2->ce_tprev != NULL) 
				(c2->ce_tprev)->ce_tnext = c;
			else c_thead = c;
			c2->ce_tprev = c;
		}
		else {
			// Insertion after C2
			c->ce_tnext = c2->ce_tnext;
			c->ce_tprev = c2;
			if (c2->ce_tnext != NULL) 
				(c2->ce_tnext)->ce_tprev = c;
			else c_ttail = c;
			c2->ce_tnext = c;
		}	
	}

	// Add object size to total size
	c_size += (c->ce_size);
	c_count++;

	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount++;
}

// Append the cache entry at the end of the list
void LRU::appendEntry(CacheEntry *c) {
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
CacheEntry * LRU::searchCache(unsigned int key) {
	int i;
	CacheEntry *c;
	
	// hash queue index 
	i = key % HASH_SIZE;

	c = c_hdrs[i];

	while ((c != NULL) && (c->ce_key != key)) {
		c = c->ce_next;
	}

	if (c != NULL) {
		deleteEntry(c);
		prependEntry(c);
	}

	return c;
}

// Search for the entry based on unsigned int, 
// but the position is not changed
CacheEntry * LRU::lookCache(unsigned int key) {
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
unsigned int LRU::getCount(void) { return c_count; }

// Total size 
unsigned long LRU::getSize(void) { return c_size; }

void LRU::displayStatistics(void) {
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
	cout << c_name << "\nCache max size \t:" << c_maxsize/1024 << "KB" 
		<< "\nCache total count of entries \t: " << c_count
		<< "\nDirty count \t\t\t: " << c_dcount <<"\nTotal size \t\t\t: " 
		<< c_size/1024 << "KB" << "\nNo of cache replacements \t: " 
		<< CACHE_REPLACEMENTS 
		<< "\nNList \t\t\t:" << HASH_SIZE 
		<< "\nMax list length \t\t:" << maxl
		<< "\nMin list length \t\t:" << minl
		<< "\nAvg list length \t\t:" << avgl << endl;
}

void LRU::display(void) {
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



#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <limits.h>

#include "darc.h"
#include "util.h"
#include "cachemem.h"

extern unsigned long long int curTime;

DARC::DARC(const char *name, unsigned int hashsize, unsigned long maxsize, unsigned int objsize, StorageNode *sn) {
	char tname[30];

	strcpy(c_name, name);
	c_count = 0;
	c_dcount = 0;
	c_size = 0;
	c_objsize = objsize;
	c_maxsize = maxsize;
	c_maxcount = maxsize / objsize;
	c_rrefs = c_wrefs = 0;
	c_sn = sn;

	HASH_SIZE = hashsize;
	CACHE_REPLACEMENTS = 0;

	p = 0;
	IP = NULL;
	below = 0;
	c_maxbelow = c_maxcount * (100.0 - IPPERCENT) / 100.0;
	
	strcpy(tname, name); 
	strcat(tname, ":T1");
	T1 = new LRUList(tname, hashsize, maxsize, sn);

	strcpy(tname, name); 
	strcat(tname, ":T2");
	T2 = new LRUList(tname, hashsize, maxsize, sn);

	strcpy(tname, name); 
	strcat(tname, ":B1");
	B1 = new LRUList(tname, hashsize, maxsize, sn);

	strcpy(tname, name); 
	strcat(tname, ":B2");
	B2 = new LRUList(tname, hashsize, maxsize, sn);
}

void DARC::decayWeights(void) {
	CacheEntry *c;

	c = T2->c_thead;

	while (c != NULL) {
		c->ce_wrefcount /= 2;
		c->ce_refcount /= 2;
		c = c->ce_tnext;
	}

	c = B2->c_thead;

	while (c != NULL) {
		c->ce_wrefcount /= 2;
		c->ce_refcount /= 2;
		c = c->ce_tnext;
	}
}

void DARC::newInterval(void) {
	c_rrefs = c_wrefs = 0;
}

unsigned int DARC::flushCache(fstream &fs, unsigned int objsize, int disk) {
	int count = 0;
	
	count = T1->flushCache(fs, objsize, disk);
	count += T2->flushCache(fs, objsize, disk);

	//c_dcount -= count;	// Should be zero
	c_dcount = 0;

	return count;
}

void DARC::disableCache(fstream &fs, unsigned int objsize, int disk) {

	cout << "Disabling cache ...\n";
	T1->disableCache(fs, objsize, disk);
	T2->disableCache(fs, objsize, disk);
	c_count = c_dcount = 0;

	cout << "Disabling cache completed ...\n";
}


float DARC::popularity(CacheEntry *c) {
	float p;
	float t;
	float wtw, wtr; //, wf;

	if (c == NULL) p = 0.0;
	else {
		t = curTime - ((c->ce_rtime > c->ce_wtime) ? c->ce_rtime : c->ce_wtime);
		if (t <= (1000 * 1000)) t = 1000 * 1000; // One mille second interval
		// Count of 10 minutes intervals
		t = (t/(1000.0 * 1000.0 * 1000.0))/(10*60); 
		if ((c_wrefs + c_rrefs) > 0) {
			wtw = c_wrefs / (c_wrefs + c_rrefs);
			wtr = c_rrefs / (c_wrefs + c_rrefs);
		}
		else wtw = wtr = 0.5;

		p = ((wtw * c->ce_wrefcount) + 
			(wtr * (c->ce_refcount - c->ce_wrefcount)));
		p = p / t;
	}

	return p;
}

// Search operation, return pointer to the found entry, if found
// Otherwise return NULL
CacheEntry * DARC::lookCache(unsigned int key) {
	CacheEntry *c;

	// Search in T1
	c = T1->lookCache(key);
	if (c != NULL) return c;

	// Search in T2	
	c = T2->lookCache(key);
	if (c != NULL) return c;

	// Not found
	return NULL;
}

// Search operation, return pointer to the found entry, if found
// Otherwise return NULL
CacheEntry * DARC::searchCache(unsigned int key) {
	CacheEntry *c;

	// Search in T1
	c = T1->searchCache(key);
	if (c != NULL) {
		T1->deleteEntry(c);
		T1->prependEntry(c);
		//movetoT2(c);
		return c;
	}

	// Search in T2	
	c = T2->searchCache(key);
	return c;
}

void DARC::repositionEntry(CacheEntry *c) {
	float popIP, popx;
	CacheEntry *c1;

	c1 = T1->searchCache(c->ce_key);
	if (c1 != NULL) {
		T1->deleteEntry(c1);
		if (c1->ce_refcount > 1) movetoT2(c1);
		else T1->prependEntry(c1);
	}
	else {

		c1 = c;
		// If the found is insertion point entry, no need of
		// changing its position
		if (c1 != IP) {
			T2->deleteEntry(c1);
			if (ARC == 1) {
				T2->prependEntry(c1);
				return;
			}
			popIP = popularity(IP);
			popx = popularity(c1);

			if ((c1->ce_flags & CACHE_ENTRY_UPPER) == 0) {
				if (below > 0) below--;
			}

			// If the entry belongs to the upper part 
			// move to MRU position
			if (c1->ce_flags & CACHE_ENTRY_UPPER)
				T2->prependEntry(c1);
			else {
				if (popx > popIP) {
					c1->ce_flags |= CACHE_ENTRY_UPPER;
					T2->prependEntry(c1);
					adjustIP();
				}
				else {
					T2->insertAtIP(IP, c1);
					c1->ce_flags &= (~CACHE_ENTRY_UPPER);
					below++;
				}
			}
		}
	}
}

void DARC::deleteEntry(CacheEntry *c) {
	CacheEntry *c1;

	// Search in T1 and T2, and delete
	c1 = T1->searchCache(c->ce_key);
	if (c1 != NULL) T1->deleteEntry(c1);
	else {
		c1 = c;
		T2->deleteEntry(c1);

		if (c1 == IP) {
			IP = NULL;
			below = 0;
		}
		else if ((c1->ce_flags & CACHE_ENTRY_UPPER) == 0) {
			if (below > 0) below--;
		}
	}

	// Decrement the count of entries
	c_count--;
	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount--;
}

// Count of entries
unsigned int DARC::getCount(void) {
	return T1->getCount() + T2->getCount();
}

// Total size 
unsigned long DARC::getSize(void) {
	return T1->getSize() + T2->getSize();
}

void DARC::displayStatistics(void) {
	cout << c_name << ":T1\n";
	T1->displayStatistics();
	cout << c_name << ":T2\n";
	T2->displayStatistics();
	cout << c_name << ":B1\n";
	B1->displayStatistics();
	cout << c_name << ":B2\n";
	B2->displayStatistics();
	cout << "Total count/dirty : " << c_count << "/" << c_dcount << endl;
}

void DARC::adjustIP(void) {
	int diff;
	unsigned int bcount;

	if (ARC == 1) return;
	if (IP == NULL) {
		IP = T2->c_ttail;
		below = 0;
		if (IP == NULL) return;
	}
	//if (below < 0) below = 0;
	
	bcount = (T2->c_count * (100.0 - IPPERCENT))/100.0;

	//cout << " below : " << below << " bcount : " << bcount << endl;
	if (bcount == below) return;

	if (bcount > below) {
		diff = bcount - below;
		below = bcount;

		while ((IP->ce_tprev != NULL) && (diff > 0)) {
			IP->ce_flags &= (~CACHE_ENTRY_UPPER);
			IP = IP->ce_tprev;
			diff--;
		}
	}
	else {
		diff = below - bcount;
		below = bcount;

		while ((IP->ce_tnext != NULL) && (diff > 0)) {
			IP = IP->ce_tnext;
			IP->ce_flags |= CACHE_ENTRY_UPPER;
			diff--;
		}
	}
}

/*
CacheEntry * DARC::addEntry(CacheEntry *c) {
	if (c->ce_refcount > 1)
		movetoT2(c);
	else	T1->prependEntry(c);

	// Increment the count of entries
	c_count++;
	if (c->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount++;
	return c;
}
*/

// Move an entry c to T2 cache
void DARC::movetoT2(CacheEntry *c) {
	float popx, popIP;

	if (ARC == 1) {
		T2->prependEntry(c);
		return;
	}

	popIP = popularity(IP);
	popx = popularity(c);

	if (popx > popIP) {
		// Move c to MRU position in T2
		T2->prependEntry(c);
		c->ce_flags |= CACHE_ENTRY_UPPER;
	
		adjustIP();	
	}
	else {
		// Move c to the position behind IP
		T2->insertAtIP(IP, c);
		c->ce_flags &= (~CACHE_ENTRY_UPPER);
		below++;
		adjustIP();
	}
}

void DARC::appendEntry(CacheEntry *c) {
	if (c->ce_refcount > 1) {
		T2->appendEntry(c);
		c->ce_flags &= (~CACHE_ENTRY_UPPER);
		below++;
	}
	else T1->appendEntry(c);
}

CacheEntry * DARC::findEntry(unsigned int *count, 
		fstream &fs, int disk, 
		int (* evictor)(fstream &fs, CacheEntry *ent, int dsk)) {
	CacheEntry *c = NULL, *ev;

	*count = 0;

	// If total number of cache entries are 
	// less than maxcount, then allocate one more entry.
	if ((T1->c_count + T2->c_count) < c_maxcount) {
		c = (CacheEntry *)c_sn->mem->cemalloc();
		return c;
	}

	// Entry not found in (T1, T2,) B1 and B2
	// Cache is full, find an entry for replacement
	if ((T1->c_count + B1->c_count) >= c_maxcount) {
		if (T1->c_count < c_maxcount) {
			// delete LRU in B1;
			c = B1->selectVictim();
			

			ev = replace(p, 0);
			
			if (evictor == NULL)
				*count = evictEntry(fs, ev, disk);
			else
				*count = evictor(fs, ev, disk);
		}
		else {
			// delete LRU in T1;
			c = T1->selectVictim();
			
			c_count--;
			if (c->ce_flags & CACHE_ENTRY_DIRTY)
				c_dcount--;
			if (evictor == NULL)
				*count = evictEntry(fs, c, disk);
			else
				*count = evictor(fs, c, disk);
		}
	}
	else {
		if ((T1->c_count + T2->c_count + B1->c_count 
			+ B2->c_count) >= c_maxcount) {
			if ((T1->c_count + T2->c_count + B1->c_count 
				+ B2->c_count) >= 2*c_maxcount) {
				// delete LRU in B2
				c = B2->selectVictim();
			}

			ev = replace(p, 0);
			if (evictor == NULL)
				*count = evictEntry(fs, ev, disk);
			else
				*count = evictor(fs, ev, disk);
		}
	}

	//cout << "findEntry " << c << endl;
	// If no entry obtained? In case the entry to be removed is
	// locked?
	if (c == NULL) {
		c = (CacheEntry *)c_sn->mem->cemalloc();
		//assert(c != NULL);
	}

	return c;
}

// Replace an entry, with the details given through parameters.
// It will make use of B1 and B2 caches, empty entries, for replacement.
// Assuming that replaceEntry will be called only when 
// search is failed to find an entry in either in T1 or in T2.
unsigned int DARC::addReplaceEntry(fstream &fs, unsigned int key, void *obj,
		unsigned short size, unsigned long long int rtime,
		unsigned long long int wtime, int disk, unsigned short flags,
		int (* evictor)(fstream &fs, CacheEntry *ent, int dsk), CacheEntry **retentry) {
	CacheEntry *c, *ev;
	int found = 0;
	int incr;
	unsigned int count = 0;

	// First search in B1 and B2.	
	c = B1->searchCache(key);
	if (c != NULL) {
		found = 3;

		// Update p, eq 2
		if (B1->c_count >= B2->c_count) incr = 1;
		else incr = (B2->c_count / B1->c_count);

		p = p + incr;
		if (((unsigned int)p) > c_maxcount) p = c_maxcount;

		B1->deleteEntry(c);
	}
	else if ((c = B2->searchCache(key)) != NULL) {
		// Search in B2, success
		found = 4;

		// Update p, eq 3
		if (B2->c_count >= B1->c_count) incr = 1;
		else incr = (B1->c_count / B2->c_count);

		p = p - incr;
		if (p < 0) p = 0;

		B2->deleteEntry(c);
	}
	
	if (c != NULL) {
		// Found entry either in B1 or B2	
		// Replace(c, p)
		if ((T1->c_count + T2->c_count) >= c_maxcount) {
			ev = replace(p, found);
			if (evictor == NULL)
				count = evictEntry(fs, ev, disk);
			else
				count = evictor(fs, ev, disk);
		}

		c->ce_object = obj;
		c->ce_size = size;
		c->ce_refcount++;
		c->ce_flags = flags;
		c->ce_lckcnt = 0;

		if (rtime != 0) {
			// Read operation
			c->ce_rtime = rtime;
			c_rrefs += 1.0;
		}
		else {
			// Write operation
			c->ce_wtime = wtime;
			c->ce_wrefcount++;
			c_wrefs += 1.0;
		}

		//movetoT2(c);
		movetoT2(c);

		c_count++;
		if (flags & CACHE_ENTRY_DIRTY) c_dcount++;
		if (retentry != NULL) *retentry = c;

		return count;
	}

	// Not found in B1 or B2
	// Find an entry from one of the lists
	c = findEntry(&count, fs, disk, evictor); 

	assert(c != NULL);
	
	c->ce_key = key;
	c->ce_rtime = rtime;
	c->ce_wtime = wtime;
	c->ce_object = obj;
	c->ce_size = size;
	c->ce_refcount = 1;
	c->ce_flags = flags;
	c->ce_lckcnt = 0;

	if (wtime != 0) {
		// Write operation 
		c->ce_wrefcount = 1;
		c_wrefs += 1.0;
	}
	else {
		c->ce_wrefcount = 0;
		c_rrefs += 1.0;
	}

	T1->prependEntry(c);
	c_count++;
	if (flags & CACHE_ENTRY_DIRTY) c_dcount++;
	if (retentry != NULL) *retentry = c;

	return count;
}

// Replace entry in list
CacheEntry * DARC::replace(unsigned int p, int found) {
	CacheEntry *c1;

	if ((T1->c_count != 0) && ((T1->c_count > p) || 
		((found == 4) && (T1->c_count >= p)))) { 
		// delete the LRU in T1
		c1 = T1->selectVictim();

		if (c1 == NULL) {
			c1 = T2->selectVictim();
			if (c1 == IP) {
				IP = NULL;
				below = 0;
			}
			B2->prependEntry(c1);
		}
		else // Move the deleted to MRU in B1
			B1->prependEntry(c1);
	}
	else {
		// delete the LRU in T2
		c1 = T2->selectVictim();
		if (c1 == NULL) {
			c1 = T1->selectVictim();
			B1->prependEntry(c1);
		}
		else {
			if (below > 0) below--;

			if (c1 == IP) {
				IP = NULL;
				below = 0;
			}

			// move the deleted to MRU in B2 
			B2->prependEntry(c1);
		}
	}

	c_count--;
	if (c1->ce_flags & CACHE_ENTRY_DIRTY)
		c_dcount--;
	// Return the replaced entry, its object must be cleared (freed)
	// at calling place.
	return c1;	
}


unsigned int DARC::evictEntry(fstream &fs, CacheEntry *c, int disk) {
	unsigned int count = 0;
	int wcount, wstart;

	if (c != NULL) {
		// Write the dirty content of evicted
		// entry to file/disk.
		if (c->ce_flags & CACHE_ENTRY_DIRTY) {
			if (c->ce_object != NULL) {
				fs.seekp((unsigned long)(c->ce_key-1)*c->ce_size);
				if (fs.fail()) {
					cout << c_name << " Error evict seek failed key " << c->ce_key << endl;
					fs.clear();
				}
				fs.write((char *)c->ce_object, c->ce_size);
		
				if (fs.fail()) {
					cout << c_name << " Error evict write failed key " << c->ce_key << endl;
					fs.clear();
				}
			}

			if (disk >= 0)  {
				wcount = c->ce_size / 512;
				wstart = (c->ce_key) * wcount;
				DiskWrite(c_sn, curTime, disk, wstart, wcount);
				count = wcount;
			}
			else count = 1;
		}

		if (c->ce_object != NULL) {
			if (c->ce_size == 512) 
				c_sn->mem->b512free((char *)c->ce_object); 
			else {
				cout << "Error : DARC evictEntry object memory size : " << c->ce_size << " not freed\n";
			}
		}
		c->ce_object = NULL;
		c->ce_flags = 0;
		c->ce_lckcnt = 0;
	}

	return count;
}

/*
// select victim, delete and return
CacheEntry * DARC::selectVictim() {
	CacheEntry *c1;

	c1 = replace(p, 0);

	return c1;	
}


*/

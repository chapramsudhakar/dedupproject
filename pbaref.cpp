#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>


#include "dedupn.h"
#include "pbaref.h"
#include "cachemem.h"
#include "bitmap.h"
#include "hbtree.h"
#include "metadata.h"
#include "storagenode.h"

#ifdef	DARCP
#include "darc.h"
extern DARC dc;
#else
#include "lru.h"
extern LRU dc;
#endif

extern HASHBTree hbt;

//extern unsigned long long int UNIQUE_BLOCK_COUNT;

extern int OPERATION;


void PbaRefOps::kvdelete(unsigned int i, PbaRefIncore *c) {
	if (c->next != NULL) (c->next)->prev = c->prev;
	if (c->prev != NULL) (c->prev)->next = c->next;
	else KVQ[i] = c->next;

	if (c->tnext != NULL) (c->tnext)->tprev = c->tprev;
	else KVTail = c->tprev;
	if (c->tprev != NULL) (c->tprev)->tnext = c->tnext;
	else KVHead = c->tnext;
}

void PbaRefOps::kvprepend(unsigned int i, PbaRefIncore *c) {
	c->prev = NULL;
	c->next = KVQ[i];
	if (KVQ[i] != NULL) KVQ[i]->prev = c;
	KVQ[i] = c;

	c->tprev = NULL;
	c->tnext = KVHead;
	if (KVHead != NULL) {
		KVHead->tprev = c;
		KVHead = c;
	}
	else KVTail = KVHead = c;
}

PbaRefIncore * PbaRefOps::kvsearch(unsigned int i, unsigned int key) {
	PbaRefIncore  *c;

	c = KVQ[i];
	while ((c != NULL) && (c->key != key))
		c = c->next;
	return c;
}

PbaRefIncore * PbaRefOps::kvalloc(void) {
	PbaRefIncore  *c = NULL;
	int i;

	if (FL != NULL) {
		// Allocate from free list
		c = FL;
		FL = FL->next;
	}
	else {
		// Replace an existing entry at the tail end
		c = KVTail;

		i = c->key % KVHASH;
		kvdelete(i, c);
		if (c->flags & CACHE_ENTRY_DIRTY) {
			pbaWrite(c);
			PBREFUPDATE++;

			if (OPERATION == OP_READ)
				PBREF_WRITECOUNT_READ++;
			else
				PBREF_WRITECOUNT_WRITE++;
		}
	}

	return c;
}

void PbaRefOps::pbaRead(unsigned int pba, PbaRefIncore *p) {
	checkSeekPos(pba);
	pref.seekg((unsigned long)(pba-1) * sizeof(struct PbaRef));
	if (pref.fail()) {
		cout << "Error: pbaRead seek failed\n";
		pref.clear();
		return;
	}

	pref.read((char *)p->hash, 16);
	pref.read((char *)&(p->ref), sizeof(unsigned int));
	p->key = pba;

	PBREFSEARCH++;

	if (OPERATION == OP_READ)
		PBREF_READCOUNT_READ++;
	else
		PBREF_READCOUNT_WRITE++;
}

void PbaRefOps::pbaWrite(PbaRefIncore *p) {
	checkSeekPos(p->key);
	pref.seekp((unsigned long)(p->key-1) * sizeof(struct PbaRef));
	if (pref.fail()) {
		cout << "Error: pbaWrite seek failed\n";
		pref.clear();
		return;
	}

	pref.write((char *)p->hash, 16);
	pref.write((char *)&(p->ref), sizeof(unsigned int));

	if (OPERATION == OP_READ)
		PBREF_WRITECOUNT_READ++;
	else
		PBREF_WRITECOUNT_WRITE++;
}

PbaRefOps::PbaRefOps(StorageNode *sn) {
	PBAREF_CACHE_HIT = 0;
	PBAREF_CACHE_MISS = 0;
	PBREFSEARCH = 0;
	PBREFINSERT = 0;
	PBREFDELETE = 0;
	PBREFUPDATE = 0;

	PBREF_READCOUNT_READ = 0;
	PBREF_WRITECOUNT_READ = 0;
	PBREF_READCOUNT_WRITE = 0;
	PBREF_WRITECOUNT_WRITE = 0;

	psn = sn;
}

PbaRefOps::PbaRefOps(const char *pbarefname, unsigned int csize, StorageNode *sn) {
	strcpy(prefname, pbarefname);
	pref.open(prefname, ios::in | ios::out | ios::binary);

	PBAREF_CACHE_HIT = 0;
	PBAREF_CACHE_MISS = 0;
	PBREFSEARCH = 0;
	PBREFINSERT = 0;
	PBREFDELETE = 0;
	PBREFUPDATE = 0;

	PBREF_READCOUNT_READ = 0;
	PBREF_WRITECOUNT_READ = 0;
	PBREF_READCOUNT_WRITE = 0;
	PBREF_WRITECOUNT_WRITE = 0;

	psn = sn;

	pbaRefCacheInit(csize);
}


void PbaRefOps::pbaRefCacheInit(unsigned int size) {
	unsigned int i, n;
	struct PbaRefIncore *p, *q;

	KVCacheSize = size;
	n = KVCacheSize / sizeof(struct PbaRefIncore);
	q = (struct PbaRefIncore *) malloc(n * sizeof(struct PbaRefIncore));
	if (q == NULL) {
		cout << " Pba Ref Cache malloc 1 failure" << endl;
		exit(0);
	}

	p = FL = q;
	p->prev = NULL;
	for (i=1; i<n; i++) {
		p->next = &q[i];
		//(p->next)->prev = p;
		p = p->next;
	}
	p->next = NULL;

	KVHASH = n/20 + 1;
	KVQ = (struct PbaRefIncore **) malloc(KVHASH * sizeof(struct PbaRefIncore *));
	if (KVQ == NULL) {
		cout << " Pba Ref Cache malloc 2 failure" << endl;
		exit(0);
	}

	for (i=0; i<KVHASH; i++) KVQ[i] = NULL;
	KVTail = KVHead = NULL;

	pref.seekg(0, pref.end);
	maxb = pref.tellg() / sizeof(struct PbaRef);
}

void PbaRefOps::checkSeekPos(unsigned int pba) {
	int i;
	struct PbaRef p;
	
	if (maxb < pba) {
		// Expand the pbaref file
		pref.seekp(0, pref.end);
		memset(&p, 0, sizeof(struct PbaRef));
		while (maxb < pba) {
			for (i=0; i<(1024*1024); i++) 
				pref.write((char *)&p, sizeof(struct PbaRef));
			maxb += (1024*1024);	
		}
	}
}

unsigned int PbaRefOps::incrementPbaRef(unsigned int pba, unsigned char *hash) {
	struct PbaRefIncore *p; 
	unsigned int i;

	i = pba % KVHASH;
	p = kvsearch(i, pba);

	if (p != NULL) {
		// Entry found in the cache
		PBAREF_CACHE_HIT++;
		kvdelete(i, p);
		p->ref++;
		p->flags |= CACHE_ENTRY_DIRTY;
		kvprepend(i, p);
	}
	else {
		PBAREF_CACHE_MISS++;
		p = kvalloc();

		pbaRead(pba, p);
		p->ref++;

		// First time modifying the entry?
		if (p->ref == 1) memcpy(p->hash, hash, 16); 

		p->flags = CACHE_ENTRY_DIRTY;

		kvprepend(i, p);
	}
	return p->ref;
}

void PbaRefOps::pbaRefZero(PbaRefIncore *p) {
	//CacheEntry *c;

	// If the reference count reaches zero remove the
	// corresponding hash entry from hash index.
	// Mark the bit map to indicate the block is free.
		
	// Delete from hash index
	psn->md->hbt->deleteValWrap(p->hash);

	// Free the pba
	psn->bmap->freeBlock(p->key);
	psn->UNIQUE_BLOCK_COUNT--;

	// Delete data block from cache, if present
	//c = dc.searchCache(p->key);
	
	//if (c != NULL) {
		//dc.deleteEntry(c);
		//psn->mem->cefree(c);
	//}

	// Clear the entry in pba ref table 
	memset(p->hash, 0, 16);
	pbaWrite(p);
	PBREFDELETE++;
}

unsigned int PbaRefOps::decrementPbaRef(unsigned int pba) {
	struct PbaRefIncore *p; 
	unsigned int retval = 0;
	unsigned int i;

	i = pba % KVHASH;
	p = kvsearch(i, pba);
	if (p != NULL) {
		PBAREF_CACHE_HIT++;
		
		// Entry found in the cache
		p->ref--;
		retval = p->ref;
		if (retval == 0) {
			// free the entry
			kvdelete(i, p);
			pbaRefZero(p);
			p->next = FL;
			FL = p;
		}
	}
	else {
		// Otherwise read the entry from 
		// the file
		PBAREF_CACHE_MISS++;
		
		p = kvalloc();

		pbaRead(pba, p);

		p->ref--;
		retval = p->ref;
		if (p->ref == 0) {
			// Release p
			pbaRefZero(p);
			p->next = FL;
			FL = p;
		}
		else {
			// Add a cache entry
			p->flags = CACHE_ENTRY_DIRTY;
			kvprepend(i, p);
		}
	}

	return retval;
}

unsigned int PbaRefOps::flushCache(void) {
	return kvflush();
}

unsigned int PbaRefOps::kvflush(void) {
	struct PbaRefIncore *c;
	unsigned int count = 0;

	c = KVTail;
	while (c != NULL) {
		if (c->flags & CACHE_ENTRY_DIRTY) {
			pbaWrite(c);
			PBREFUPDATE++;
			c->flags ^= CACHE_ENTRY_DIRTY;
			count++;
		}

		c = c->tprev;
	}
	return count;
}


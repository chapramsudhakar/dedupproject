#include <iostream>
#include <fstream>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "dedupn.h"

#include "lru.h"

#include "filerecipe.h"
#include "cachemem.h"
#include "storagenode.h"
#include "util.h"

extern unsigned long long int curTime;

extern int OPERATION;

// Cache flush operation
void IIIBTree::flushCache(int disk) {
	unsigned int wrcount;

	flushValWrap();
	wrcount = bt_btcache->flushCache(bt_fs, sizeof(IntIntIntBTNode), disk);
	BT_BTNODE_EFFECTIVE_WRITES += wrcount;
	BT_BTNODE_FLUSH_WRITES += wrcount;
}

void IIIBTree::displayStatistics(void) {
	cout << "Btree key count \t\t\t:" << keycount << endl
		<< "Btree node read count \t\t:" << BT_BTNODE_READCOUNT << endl
		<<  "Btree node read hit \t\t:" << BT_BTNODE_READ_CACHE_HIT << endl
		<< "Btree node write count \t\t:" << BT_BTNODE_WRITECOUNT << endl
		<< "Btree node write hit \t\t:" << BT_BTNODE_WRITE_CACHE_HIT << endl
		<< "Btree node effective reads \t:" << (BT_BTNODE_READCOUNT - BT_BTNODE_READ_CACHE_HIT) << endl
		<< "Btree node effective writes \t:" << BT_BTNODE_EFFECTIVE_WRITES << endl
		<< "Btree node flush writes \t:" << BT_BTNODE_FLUSH_WRITES << endl
		<< "Btree read operation read count\t:" << BT_BTNODE_READCOUNT_READ << endl
		<< "Btree read operation write count\t:" << BT_BTNODE_WRITECOUNT_READ << endl
		<< "Btree write operation read count\t:" << BT_BTNODE_READCOUNT_WRITE << endl
		<< "Btree write operation write count\t:" << BT_BTNODE_WRITECOUNT_WRITE << endl
		<< "Btree BTINSERT\t\t\t:" << BTINSERT << endl
		<< "Btree BTSEARCH\t\t\t:" << BTSEARCH << endl
		<< "Btree BTUPDATE\t\t\t:" << BTUPDATE << endl
		<< "Btree BTDELETE\t\t\t:" << BTDELETE << endl
		<< "Btree node usage \t\t:" << bt_root.btn_ucount << "/" << bt_maxblks << " out of " << bt_nblks << endl;
	bt_btcache->displayStatistics();
}

void IIIBTree::enableCache(unsigned int size) {
#ifdef	DARCP
	 bt_btcache = new DARC(bt_fname, bt_hashsize, size, 512, bt_sn);
#else
	 bt_btcache = new LRU(bt_fname, bt_hashsize, size, bt_sn);
#endif
}

void IIIBTree::enableKeyValCache(unsigned int size) {
	unsigned int i, n;
	struct IIIBTVal *p, *q;

	n = size / sizeof(struct IIIBTVal);
	KVCacheSize = size;
	q = (struct IIIBTVal *) malloc(n * sizeof(struct IIIBTVal));
	if (q == NULL) {
		cout << bt_fname << " : keyValueCache malloc 1 failure" << endl;
		exit(0);
	}
	p = FL = q;
	p->prev = NULL;
	for (i=1; i<n; i++) {
		p->next = &q[i];
		p = p->next;
	}
	p->next = NULL;

	KVHASH = n/20 + 1;
	KVQ = (struct IIIBTVal **) malloc(KVHASH * sizeof(struct IIIBTVal *));
	if (KVQ == NULL) {
		cout << bt_fname << " : keyValueCache malloc 2 failure" << endl;
		exit(0);
	}
	for (i=0; i<KVHASH; i++) KVQ[i] = NULL;
	KVTail = KVHead = NULL;
}

// Destructor, writes root node and closes the index file
IIIBTree::~IIIBTree() { 
	putNode(1, &bt_root); 

	if (bt_btcache != NULL) {
		bt_btcache->disableCache(bt_fs, sizeof(IntIntIntBTNode), METADATADISK);
		delete bt_btcache;
	}

	bt_fs.close(); 
}

IIIBTree::IIIBTree(const char fname[100], unsigned int mblocks, unsigned int hashsize, unsigned int incr, StorageNode *sn) { 
	// Read the index details from the fname file
	strcpy(bt_fname, fname);
	bt_maxblks = mblocks;	// Find the current size and correct this 
				// initial value
	bt_incr = incr;
	bt_btcache = NULL;
	bt_hashsize = hashsize;
	bt_sn = sn;

	BT_BTNODE_READCOUNT = 0;
	BT_BTNODE_READ_CACHE_HIT = 0;
	BT_BTNODE_WRITECOUNT = 0;
	BT_BTNODE_WRITE_CACHE_HIT = 0;
	BT_BTNODE_EFFECTIVE_WRITES = 0;
	BT_BTNODE_FLUSH_WRITES = 0;

	BT_BTNODE_READCOUNT_READ = 0;
	BT_BTNODE_WRITECOUNT_READ = 0;
	BT_BTNODE_READCOUNT_WRITE = 0;
	BT_BTNODE_WRITECOUNT_WRITE = 0;

	BTSEARCH = 0;
	BTUPDATE = 0;
	BTINSERT = 0;
	BTDELETE = 0;

	nextFreeIIIBTreeNode = 2;
	nodeCount = 0;
	maxlevel = 0;
	keycount = 0;

	bt_fs.open(bt_fname, ios::in | ios::out | ios::binary);
	
	if (bt_fs.fail()) {
		cout << "ERROR Index file opening failure : " << bt_fname << endl;
		exit(0);
	}
	bt_fs.clear();

	assert(readNode(1, &bt_root) == 0);
	flist = new FreeList(fname,512);

	// Find the current maximum number of blocks in the index
	bt_fs.seekg(0, bt_fs.end);
	bt_maxblks = bt_fs.tellg() / sizeof(IntIntIntBTNode);
	
	FL = NULL;
	KVQ = NULL;
	KVTail = KVHead = NULL;
}

int IIIBTree::putNode(unsigned int n, IntIntIntBTNode *b) {
	CacheEntry *c;
	IntIntIntBTNode *b1;
	unsigned int wrcount;

	assert(n > 0);

	// Try to put into the cache if space is available
	if (bt_btcache != NULL) {
		BT_BTNODE_WRITECOUNT++;
		c = bt_btcache->searchCache(n);

		if (c != NULL) {
			lockEntry(c);
			BT_BTNODE_WRITE_CACHE_HIT++;
			// Update object
			if (c->ce_object != b)
				memcpy(c->ce_object, b, sizeof(IntIntIntBTNode));

			c->ce_wtime = curTime; 
			if (!(c->ce_flags & CACHE_ENTRY_DIRTY)) {
				c->ce_flags |= CACHE_ENTRY_DIRTY;
				bt_btcache->c_dcount++;
			}
			c->ce_refcount++;
			c->ce_wrefcount++;
			bt_btcache->c_wrefs++;
			bt_btcache->repositionEntry(c);
			unlockEntry(c);
		}
		else {
			b1 = (IntIntIntBTNode *) bt_sn->mem->b512malloc();
			//btalloc++;
			assert(b1 != NULL);
			memcpy(b1, b, sizeof(IntIntIntBTNode));
			
			//cout << bt_fname << "ibtree1 n " << n << " addr " << static_cast<void *>(b1) << endl;
			//if ((bt_btcache->c_size + 512) > bt_btcache->c_maxsize) btfree++;
			wrcount = bt_btcache->addReplaceEntry(bt_fs, n, (void *)b1, 
				sizeof(IntIntIntBTNode), 0, curTime, METADATADISK, CACHE_ENTRY_DIRTY, NULL, &c);
			BT_BTNODE_EFFECTIVE_WRITES += wrcount;

			if (OPERATION == OPERATION_READ)
				BT_BTNODE_WRITECOUNT_READ += wrcount;
			else 
				BT_BTNODE_WRITECOUNT_WRITE += wrcount;
		}
	}
	else return writeNode(n, b);

	return 0;	
}

IntIntIntBTNode * IIIBTree::getNode(unsigned int n) {
	CacheEntry *c;
	IntIntIntBTNode *b1 = NULL;
	unsigned int wrcount;

	assert(n > 0);

	// First check in the cache 
	if (bt_btcache != NULL) {
		c = bt_btcache->searchCache(n);
		if (c != NULL) {
			BT_BTNODE_READCOUNT++;
			BT_BTNODE_READ_CACHE_HIT++;
			c->ce_refcount++;
			c->ce_rtime = curTime;
			bt_btcache->c_rrefs++;
			bt_btcache->repositionEntry(c);
			b1 = (IntIntIntBTNode *)c->ce_object;
		}
		else {
			// Read block and add to cache
			b1 = (IntIntIntBTNode *)bt_sn->mem->b512malloc();
			//cout << bt_fname << "ibtree2 n " << n << " addr " << static_cast<void *>(b1) << endl;
			//btalloc++;
			assert(b1 != NULL);
			assert(readNode(n, b1) == 0);

			//if ((bt_btcache->c_size + 512) > bt_btcache->c_maxsize) btfree++;
			wrcount = bt_btcache->addReplaceEntry(bt_fs, n, (void *)b1, 
				sizeof(IntIntIntBTNode), curTime, 0, METADATADISK, 
				CACHE_ENTRY_CLEAN, NULL, &c);
			BT_BTNODE_EFFECTIVE_WRITES += wrcount;
			if (OPERATION == OPERATION_READ)
				BT_BTNODE_WRITECOUNT_READ += wrcount;
			else 
				BT_BTNODE_WRITECOUNT_WRITE += wrcount;
		}
		lockEntry(c);
	}
	else {
		// Read block and add to cache
		b1 = (IntIntIntBTNode *)bt_sn->mem->b512malloc();
		//cout << bt_fname << "ibtree3 n " << n << " addr " << static_cast<void *>(b1) << endl;
		//btalloc++;
		assert(b1 != NULL);
		assert(readNode(n, b1) == 0);
	}
		
	return b1;
}

void IIIBTree::releaseNode(unsigned int n, unsigned short dirty) {
	CacheEntry *c;

	c = bt_btcache->lookCache(n);
	if (dirty != 0) {
		if (!(c->ce_flags & CACHE_ENTRY_DIRTY)) 
			bt_btcache->c_dcount++;
		c->ce_flags |= dirty;
		BT_BTNODE_WRITECOUNT++;
		BT_BTNODE_WRITE_CACHE_HIT++;
	}
	unlockEntry(c);
}

// Read a node from the index file
int IIIBTree::readNode(unsigned int n, IntIntIntBTNode *b) {

	assert(n > 0);
	BT_BTNODE_READCOUNT++;

	if (OPERATION == OPERATION_READ)
		BT_BTNODE_READCOUNT_READ++;
	else 
		BT_BTNODE_READCOUNT_WRITE++;

	bt_fs.seekg((unsigned long)(n-1)*sizeof(IntIntIntBTNode));
	if (bt_fs.fail()) {
		cout << bt_fname << " Error: readNode seekg failed : " << n << endl;
		bt_fs.clear();
		return -1;
	}
	
	bt_fs.read((char *)b, sizeof(IntIntIntBTNode));
	DiskRead(bt_sn, curTime, METADATADISK, n, 1);
	if (bt_fs.fail()) {
		cout << bt_fname << " Error: readNode read failed :" << n << endl;
		bt_fs.clear();
		return -1;
	}

	return 0;	// Success
}

// Write a node into the index file
int IIIBTree::writeNode(unsigned int n, IntIntIntBTNode *b) {

	assert(n > 0);
	BT_BTNODE_WRITECOUNT++;

	if (OPERATION == OPERATION_READ)
		BT_BTNODE_WRITECOUNT_READ++;
	else 
		BT_BTNODE_WRITECOUNT_WRITE++;

	// Otherwise write it immediately
	bt_fs.seekp((unsigned long)(n-1)*sizeof(IntIntIntBTNode));
	if (bt_fs.fail()) {
		cout << bt_fname << " Error: writeNode seekp failed\n";
		bt_fs.clear();
		return -1;
	}

	bt_fs.write((char *)b, sizeof(IntIntIntBTNode));
	DiskWrite(bt_sn, curTime, METADATADISK, n, 1);
	if (bt_fs.fail()) {
		cout << bt_fname << " Error: writeNode write failed :" << n << endl;
		bt_fs.clear();
		return -1;
	}

	return 0;	// Success
}

// Free node by writing zero bytes in the node
void IIIBTree::freeNode(unsigned int n) {
	IntIntIntBTNode *b;

	// make the entire node content zeros. 
	// (Initial unallocated state)
	b = getNode(n);
	memset((char *)b, 0, sizeof(IntIntIntBTNode));
	b->btn_leaf = 1;
	releaseNode(n, CACHE_ENTRY_DIRTY);

	bt_root.btn_ucount--;
	flist->freeVal(n);
}

// Initialize the index file
void IIIBTree::initialize(void) {
	// Only once for the first time running of the program 
	// this should be called.
	unsigned int i;
	IntIntIntBTNode b;

	cout << " initialize \n";
	memset((void *)&b, 0, sizeof(IntIntIntBTNode));
	b.btn_leaf = 1;
	bt_fs.seekp(0);

	bt_fs.clear();
	for (i=1; i<=bt_maxblks; i++) {
		if (putNode(i, &b) < 0) { 
			// Failed
			cout << " Initialize failed : Written " 
				<< bt_fs.gcount() << endl;
			exit(0);
		}
	}
	
	cout << (i-1) << " number of nodes are added\n";

	// Write the initialized root nodes
	assert(readNode(1, &bt_root) == 0);
	bt_root.btn_ucount = 1; 
	// Root node only one node is under use initially
	bt_root.btn_used = 1;
	writeNode(1, &bt_root);
	displayNode(&bt_root, 1, -1);
}

// Find a one free (unused) node
unsigned int IIIBTree::findFreeNode(void) {
	IntIntIntBTNode *b, b1;
	unsigned int i;
	unsigned fnext; // = 2;
	unsigned int ofnext;
	CacheEntry *c;

	if ((i = flist->getVal()) > 0) {
		b = getNode(i);
		b->btn_used = 1;
		releaseNode(i, CACHE_ENTRY_DIRTY);
		bt_root.btn_ucount++;
		return i;
	}

	fnext = nextFreeIIIBTreeNode;
	ofnext = fnext;

	for (i=fnext; i<= bt_maxblks; i++) {
		c = bt_btcache->lookCache(i);
		if (c != NULL) b = (struct IntIntIntBTNode *) c->ce_object;
		else {
			readNode(i, &b1);
			b = &b1;
		}
		if (!(b->btn_used)) {
			bt_root.btn_ucount++;
			nextFreeIIIBTreeNode = i+1;
			if (nextFreeIIIBTreeNode > bt_maxblks) nextFreeIIIBTreeNode = 2;
			b = getNode(i);
			b->btn_used = 1;
			releaseNode(i, CACHE_ENTRY_DIRTY);
			return i;
		}
	}

	if ((bt_root.btn_ucount + 40) < bt_maxblks) {
		for (i=2; i<ofnext; i++) {
			c = bt_btcache->lookCache(i);
			if (c != NULL) b = (struct IntIntIntBTNode *) c->ce_object;
			else {
				readNode(i, &b1);
				b = &b1;
			}
			if (!(b->btn_used)) {
				bt_root.btn_ucount++;
				nextFreeIIIBTreeNode = i+1;
				b = getNode(i);
				b->btn_used = 1;
				releaseNode(i, CACHE_ENTRY_DIRTY);
				return i;
			}
		}
	}

	// Extend the index size by bt_incr number of blocks
	memset((void *)&b1, 0, sizeof(IntIntIntBTNode));
	b1.btn_leaf = 1;
	bt_fs.seekp((bt_maxblks) * sizeof(IntIntIntBTNode));
	for (i=0; i<bt_incr; i++)
		bt_fs.write((char *)&b1, sizeof(IntIntIntBTNode));
	i = bt_maxblks + 1;
	bt_maxblks += bt_incr;

	nextFreeIIIBTreeNode = i+1;
	b = getNode(i);
	b->btn_used = 1;
	releaseNode(i, CACHE_ENTRY_DIRTY);
	return i;
}

// During insertion of a value the child node may need to be split
void IIIBTree::splitChild(IntIntIntBTNode *par, unsigned int parNo,
		int i, IntIntIntBTNode *child, unsigned int childNo) {
	unsigned int newNo;
	IntIntIntBTNode *b;
	int j;

	newNo = findFreeNode();
	assert(newNo > 0);

	b = getNode(newNo);
	
	b->btn_leaf = child->btn_leaf;
	b->btn_count = IIHMINDEGREE - 1;		// Min degree - 1
	b->btn_used = 1;

	for (j=0; j<IIHMINDEGREE-1; j++) {
		b->btn_key[j] = child->btn_key[j+IIHMINDEGREE];
		b->btn_value[j] = child->btn_value[j+IIHMINDEGREE];
		b->btn_value1[j] = child->btn_value1[j+IIHMINDEGREE];
	}

	if (!(child->btn_leaf))
		for (j=0; j<IIHMINDEGREE; j++)
			b->btn_ptrs[j] = child->btn_ptrs[j+IIHMINDEGREE];

	child->btn_count = IIHMINDEGREE - 1;
	for (j=par->btn_count; j>=i+1; j--)
		par->btn_ptrs[j+1] = par->btn_ptrs[j];
	par->btn_ptrs[i+1] = newNo;

	for (j=par->btn_count-1; j>=i; j--) {
		par->btn_key[j+1] = par->btn_key[j];
		par->btn_value[j+1] = par->btn_value[j];
		par->btn_value1[j+1] = par->btn_value1[j];
	}

	par->btn_key[i] = child->btn_key[IIHMINDEGREE-1];
	par->btn_value[i] = child->btn_value[IIHMINDEGREE-1];
	par->btn_value1[i] = child->btn_value1[IIHMINDEGREE-1];
	par->btn_count++;

	// Write nodes parent, child , new node
	releaseNode(newNo, CACHE_ENTRY_DIRTY);
	putNode(childNo, child);
	putNode(parNo, par);

	return;
}

// Insert a key and its address in a non-full Node
void IIIBTree::insertNonFull(IntIntIntBTNode *b, unsigned int bNo, 
		unsigned int key, unsigned int value, unsigned int value1) {
	int i;
	IntIntIntBTNode *b2;
	int done = 0;
	unsigned int bn2, bn3, oldbno;

	oldbno = bNo;
	while (!done) {
		i = b->btn_count-1;
		if (b->btn_leaf) {
			while ((i >= 0) && (key < b->btn_key[i])) {
				b->btn_key[i+1] = b->btn_key[i];
				b->btn_value[i+1] = b->btn_value[i];
				b->btn_value1[i+1] = b->btn_value1[i];
				i--;
			}
			if ((i < 0) || (b->btn_key[i] != key)) {
				// Distinct key?
				b->btn_key[i+1] = key;
				b->btn_value[i+1] = value;
				b->btn_value1[i+1] = value1;
				b->btn_count++;
			}
			else {
				cout << bt_fname << bt_fname << " Error file recipebtree - Duplicate key inserted : " << key << " value " << value << " oldvalue " << b->btn_value[i] << endl;
				b->btn_value[i] = value; // Updated value
				b->btn_value1[i] = value1; // Updated value
				//shift them back
				i++;
				while (i < b->btn_count) {
					b->btn_key[i] = b->btn_key[i+1];
					b->btn_value[i] = b->btn_value[i+1];
					b->btn_value1[i] = b->btn_value1[i+1];
					i++;
				}
				
			}
			putNode(bNo, b);
			done = 1;
			if (bNo != oldbno)
				releaseNode(bNo, CACHE_ENTRY_DIRTY);
		}
		else {
			while ((i >= 0) && (key < b->btn_key[i])) 
				i--;

			i++;
			
			bn2 = b->btn_ptrs[i];
			b2 = getNode(bn2);
#ifdef	__DEBUG__
			//cout << bt_fname << " before insert non full "  
				//<< bNo << " bn2 " << bn2 << " bn3 " << bn3
				//<< " leaf " << b->btn_leaf << " i "  
				//<< i << endl;
#endif
			if (b2->btn_count == IIHMAXKEYS) {
				splitChild(b, bNo, i, b2, b->btn_ptrs[i]);
				if (key > b->btn_key[i]) i++;
			}
			releaseNode(bn2, CACHE_ENTRY_DIRTY);
			// pointer i of b might have been changed!
			bn3 = b->btn_ptrs[i];

#ifdef	__DEBUG__
			//cout << bt_fname << " after insert non full "
				//<< bNo << " bn2 " << bn2 << " bn3 " << bn3
				//<< " leaf " << b->btn_leaf << " i "
				//<< i << endl;
#endif

			if (bNo != oldbno)
				releaseNode(bNo, CACHE_ENTRY_DIRTY);
			bNo = bn3;
			b = getNode(bNo);
		}
	}

	return;
}

void IIIBTree::updateVal(unsigned int key, unsigned int newval, 
	unsigned int newval1, IntIntIntBTNode *node, unsigned int nodeNo) {
	int i; //,j;
	IntIntIntBTNode *b;
	int done = 0;
	unsigned int oldnodeno;

	oldnodeno = nodeNo;

	while ((!done) && (node != NULL)) {
		i = 0;

		while ((i < node->btn_count) && 
			(key > node->btn_key[i])) {
			i++;
		}	

		// If found in the current node
		if ((i < node->btn_count) && 
			(key ==  node->btn_key[i])) {
			node->btn_value[i] = newval;
			node->btn_value1[i] = newval1;
			if (nodeNo != 1)
				putNode(nodeNo, node);
			// Else, if root node within bt_root val is modified
			done = 1;
			if (nodeNo != oldnodeno)
				releaseNode(nodeNo, CACHE_ENTRY_DIRTY);
		}
		else {
			if (node->btn_leaf) {
				break;
			}
			else {
				if (nodeNo != oldnodeno)
					releaseNode(nodeNo, 0);
				nodeNo = node->btn_ptrs[i];
				if (nodeNo > 0) {
					b = getNode(nodeNo);
					node = b;
				}
				else node = NULL;
			}	
		}
	}

	if (!done) insertVal(key, newval, newval1);
	return;
}


// Update the value of an exisiting key in the node at the 
// specified index position
void IIIBTree::updateVal(unsigned int key, unsigned int nodeNo, unsigned int pos, unsigned int val, unsigned int val1) {
	IntIntIntBTNode *b;

	if (nodeNo != 1) {
		b = getNode(nodeNo);
	
		// Check if key is matching or not
		if (b->btn_key[pos] != key) 
			cout << bt_fname << " Error ibtree: Update value key doesnot match\n";
		b->btn_value[pos] = val;
		b->btn_value1[pos] = val1;
		releaseNode(nodeNo, CACHE_ENTRY_DIRTY);
	}
	else {
		bt_root.btn_value[pos] = val;
		bt_root.btn_value1[pos] = val1;
		putNode(nodeNo, &bt_root);
	}
}

// Insert a key and its address
void IIIBTree::insertVal(unsigned int key, unsigned int value,
	unsigned int value1) {
	unsigned newNo;
	IntIntIntBTNode *b1;

	keycount++;
	if (bt_root.btn_count == (2 * IIHMINDEGREE - 1)) {
		// new node will be used to copy the old root and 
		// the first node in the index file continues to be the root
		newNo = findFreeNode();
		assert(newNo > 0);
		
		b1 = getNode(newNo);
		memcpy(b1, &bt_root, sizeof(IntIntIntBTNode));

		// Initialize the new root
		memset((char *)&bt_root, 0, sizeof(IntIntIntBTNode));
		bt_root.btn_leaf = 0;
		bt_root.btn_count = 0;
		bt_root.btn_ptrs[0] = newNo;
		bt_root.btn_used = 1;
		bt_root.btn_ucount = b1->btn_ucount;

		splitChild(&bt_root, 1, 0, b1, newNo);
		insertNonFull(&bt_root, 1, key, value, value1);
		releaseNode(newNo, CACHE_ENTRY_DIRTY);
	}
	else {
		insertNonFull(&bt_root, 1, key, value, value1);
	}
	return;
}

// Left shift value and value elements position 'from' to 'to' 
void IIIBTree::leftShiftVal(IntIntIntBTNode *b, int from, int to, int count) {
	int i;

	for (i=0; i<count; i++) {
		b->btn_key[i+to] = b->btn_key[i+from];
		b->btn_value[i+to] = b->btn_value[i+from];
		b->btn_value1[i+to] = b->btn_value1[i+from];
	}
}

// Left shift ptrs position 'from' to 'to' 
void IIIBTree::leftShiftPtrs(IntIntIntBTNode *b, int from, int to, int count) {
	int i;

	for (i=0; i<count; i++) 
		b->btn_ptrs[i+to] = b->btn_ptrs[i+from];
}

// right shift value and value elements position 'from' to 'to'
void IIIBTree::rightShiftVal(IntIntIntBTNode *b, int from, int to, int count) {
	int i;

	for (i=0; i<count; i++) {
		b->btn_key[to] = b->btn_key[from];
		b->btn_value[to] = b->btn_value[from];
		b->btn_value1[to] = b->btn_value1[from];
		from--; to--;
	}
}

// right shift ptrs position 'from' to 'to'
void IIIBTree::rightShiftPtrs(IntIntIntBTNode *b, int from, int to, int count) {
	int i;

	for (i=0; i<count; i++) 
		b->btn_ptrs[to--] = b->btn_ptrs[from--];
}

// Copy ptrs from b1 to b2 from position 'from' to 'to'
void IIIBTree::copyVal(IntIntIntBTNode *b1, IntIntIntBTNode *b2, 
			int from, int to, int count) {
	int i;

	for (i=0; i<count; i++) {
		b2->btn_key[i+to] = b1->btn_key[i+from];
		b2->btn_value[i+to] = b1->btn_value[i+from];
		b2->btn_value1[i+to] = b1->btn_value1[i+from];
	}
}

// Copy val and value elements from b1 to b2 from position 'from' to 'to'
void IIIBTree::copyPtrs(IntIntIntBTNode *b1, IntIntIntBTNode *b2, 
			int from, int to, int count) {
	int i;

	for (i=0; i<count; i++) 
		b2->btn_ptrs[i+to] = b1->btn_ptrs[i+from];
}

// Modified root/root of a subtree whose child is y is 
// to be written with proper checking.
void IIIBTree::writeModifiedRoot(IntIntIntBTNode *x, unsigned xno,
			IntIntIntBTNode *y, unsigned int *yno) {

	if (x->btn_count > 0)
		putNode(xno, x);
	else {
		// Root node is getting deleted.
		// Write y node in root position
		// and free y node
		y->btn_ucount = x->btn_ucount;
		putNode(1, y);
		memcpy(&bt_root, y, sizeof(struct IntIntIntBTNode));

		// free old y node.      
		freeNode(*yno);

		// New y node number
		*yno = 1;
	}
}

// Find maximum key and its value
void IIIBTree::findMax(unsigned int *key, unsigned int *value, unsigned int *value1, IntIntIntBTNode *x, unsigned int xno) {
	unsigned int nodeNo;
	int done = 0;
	unsigned int oldxno;

	oldxno = xno;	

	while (!done) {
		if (x->btn_leaf) {
			// If x is leaf node right most key is the maximum one
			*key = x->btn_key[x->btn_count-1];
			*value = x->btn_value[x->btn_count-1];
			*value1 = x->btn_value1[x->btn_count-1];
			done = 1;
			if (xno != oldxno)
				releaseNode(xno, 0);
		}
		else {
			// Follow the rightmost branch
			nodeNo = x->btn_ptrs[x->btn_count];
			x = getNode(nodeNo);
			xno = nodeNo;
		}
	}
}


// Find minimum key and its value
void IIIBTree::findMin(unsigned int *key, unsigned int *value, unsigned int *value1, IntIntIntBTNode *x, unsigned int xno) {
	unsigned int nodeNo;
	int done = 0;
	unsigned int oldxno;

	oldxno = xno;	

	while (!done) {
		if (x->btn_leaf) {
			// If x is leaf node left most key is the minimum one
			*key = x->btn_key[0];
			*value = x->btn_value[0];
			*value1 = x->btn_value1[0];
			done = 1;
			if (xno != oldxno)
				releaseNode(xno, 0);
		}
		else {
			// Follow the leftmost branch
			nodeNo = x->btn_ptrs[0];
			x = getNode(nodeNo);
			xno = nodeNo;
		}
	}
}

int IIIBTree::deleteVal(unsigned int key, unsigned int value, unsigned int value1, IntIntIntBTNode *x, unsigned int xno) {
	keycount--;
	return deleteVal1(key, value, value1, x, xno);
}
// Deletion of sepcified key if found
int IIIBTree::deleteVal1(unsigned int key, unsigned int value, unsigned int value1, IntIntIntBTNode *x, unsigned int xno) {
	int pos, i,j;
	unsigned int yno, zno, cno, clno, crno, oldyno, oldcno;
	IntIntIntBTNode *b1, *b2, *y, *z, *c, *cl = NULL, *cr;
	unsigned int key1;
	unsigned int value11=0, value12=0;
	int retval;

	if (x == NULL) {
		cout << bt_fname <<  " Error ibtree : deleteVal key not found " << key << endl;
		return 0;	// Not found.
	}

	// Search for the value in the node x
	for (pos = 0; pos < x->btn_count; pos++) {
		if (key <= x->btn_key[pos]) break;
	}
	if ((pos < x->btn_count) && (key == x->btn_key[pos])) {
		// value found in the node
		if (x->btn_leaf) {
			// From the leaf value can be deleted
			leftShiftVal(x, pos+1, pos, x->btn_count-pos-1);
			x->btn_count--;
			
			// This modified node should be written to index file.
			putNode(xno, x);
			return 1;
		}
		else {
			// Find whether the left node has more than IIHMINKEYS
			yno = x->btn_ptrs[pos];
			b1 = getNode(yno);
			y = b1;
			if (y->btn_count > IIHMINKEYS) {
				// Find predecessor key' in y and replace key 
				// in x, and delete key' in y.
				findMax(&key1, &value11, &value12, y, yno);
				x->btn_key[pos] = key1;
				x->btn_value[pos] = value11;
				x->btn_value1[pos] = value12;

				// Modified x node should be written to 
				// index file.
				putNode(xno, x);

				// Recursively delete key' in y
				deleteVal1(key1, value11, value12, y, yno);
				releaseNode(yno, CACHE_ENTRY_DIRTY);
				return 1;
			}
			else {
				// Find if the right node has more than IIHMINKEYS
				zno = x->btn_ptrs[pos+1];
				b2 = getNode(zno);
				z = b2;

				if (z->btn_count > IIHMINKEYS) {
					// Find successor key' in z and replace
					// key in x, and delete key' in z.
					findMin(&key1, &value11, &value12, z, zno);
					x->btn_key[pos] = key1;
					x->btn_value[pos] = value11;
					x->btn_value1[pos] = value12;

					// Modified x node should be written to 
					// index file.
					putNode(xno, x);

					// Recursively delete key' in z
					deleteVal1(key1, value11, value12, z, zno);
					releaseNode(zno, CACHE_ENTRY_DIRTY);
					releaseNode(yno, 0);
					return 1;
				}
				else {
					// merge key, z into y.
					// delete z. Delete key from y.
					i = y->btn_count;
					
					// Copy key from x into y
					y->btn_key[i] = key;
					y->btn_value[i] = x->btn_value[pos];
					y->btn_value1[i] = x->btn_value1[pos];

					i++;
					// Copy z node into y
					copyVal(z, y, 0, i, z->btn_count);
					copyPtrs(z, y, 0, i, z->btn_count+1);

				      	y->btn_count += (1 + z->btn_count);

					// Free node z
					freeNode(zno);
					releaseNode(zno, CACHE_ENTRY_DIRTY);

					// Modify x node	
					leftShiftVal(x, pos+1, pos, x->btn_count-pos-1);
					leftShiftPtrs(x, pos+2, pos+1, x->btn_count-pos-1);
					x->btn_count--;
					// Write modified node x (and y)
					oldyno = yno;
					writeModifiedRoot(x, xno, y, &yno);

					// Recursively delete key from y
					if (yno != 1) {
						putNode(yno, y);
						retval = deleteVal1(key, value, value1, y, yno);
						releaseNode(yno, CACHE_ENTRY_DIRTY);
						return retval;
					}
					else {
						releaseNode(oldyno, 0);
						retval = deleteVal1(key, value, value1, &bt_root, 1);
					       	return retval;
					}
				}
			}
		}
	}
	else {
		// key not found in x.
		// Find the aappropriate child tree that may contain key.
		if (x->btn_leaf) {
			cout << bt_fname << " Error ibtree : deleteval 2 key not found " << key << endl;
			return 0;
		}

		// In all cases follow the child at pos
		cno = x->btn_ptrs[pos];
		b1 = getNode(cno);
		c = b1;

		// If c has exactly IIHMINKEYS 
		if (c->btn_count == IIHMINKEYS) {
			clno = crno = 0;

			// Find whether left sibling has more keys
			if (pos > 0) {
				clno = x->btn_ptrs[pos-1];
				b2 = getNode(clno);
				cl = b2;
				if (cl->btn_count > IIHMINKEYS) {
					// Shift right all the values
					rightShiftVal(c, c->btn_count-1, c->btn_count, c->btn_count);
					rightShiftPtrs(c, c->btn_count, c->btn_count+1, c->btn_count+1);
					// Get a key from x.
					c->btn_value[0] = x->btn_value[pos-1];
					c->btn_value1[0] = x->btn_value1[pos-1];
					c->btn_key[0] = x->btn_key[pos-1];
					// Get right most ptr of left sibling
					c->btn_ptrs[0] = cl->btn_ptrs[cl->btn_count];
					// Copy right most value of left sibling to x
					x->btn_key[pos-1] = cl->btn_key[cl->btn_count-1]; 
					x->btn_value[pos-1] = cl->btn_value[cl->btn_count-1]; 
					x->btn_value1[pos-1] = cl->btn_value1[cl->btn_count-1]; 
					cl->btn_count--;
					c->btn_count++;
					// Write back x and cl
					putNode(xno, x);
					putNode(clno, cl);
					putNode(cno, c);
					releaseNode(clno, CACHE_ENTRY_DIRTY);
						
					// Delete key from child tree
					retval = deleteVal1(key, value, value1,c, cno);
					releaseNode(cno, CACHE_ENTRY_DIRTY);
					return retval;
				}
			}

			// If left sibling has not satisfied the condition
			// Check if right sibling has more than IIHMINKEYS
			if (pos < x->btn_count) {
				crno = x->btn_ptrs[pos+1];
				b2 = getNode(crno);
				cr = b2;
				if (cr->btn_count > IIHMINKEYS) {
					i = c->btn_count;
					// Get a key from x.
					c->btn_value[i] = x->btn_value[pos];
					c->btn_value1[i] = x->btn_value1[pos];
					c->btn_key[i] = x->btn_key[pos];
					c->btn_ptrs[i+1] = cr->btn_ptrs[0];
					x->btn_key[pos] = cr->btn_key[0];
					x->btn_value[pos] = cr->btn_value[0];
					x->btn_value1[pos] = cr->btn_value1[0];
					// Shift left the values of right sibling cr
					leftShiftVal(cr, 1, 0, cr->btn_count-1);
					leftShiftPtrs(cr, 1, 0, cr->btn_count);
					c->btn_count++;
					cr->btn_count--;
					// Write back x and cr
					putNode(xno, x);
					putNode(crno, cr);
					putNode(cno, c);
					releaseNode(crno, CACHE_ENTRY_DIRTY);
					if (clno != 0) 
						releaseNode(clno, 0);
					
					// Delete key from child tree
					retval = deleteVal1(key, value, value1, c, cno);
					releaseNode(cno, CACHE_ENTRY_DIRTY);
					return retval;
				}
			}

			// If both siblings have exactly IIHMINKEYS then merge 
			// c with one sibling. Move a key from x down to the 
			// new merged node median position.
			if (crno != 0) {
				// Merge with right
				j = c->btn_count;	

				// Copy median value from x.
				c->btn_key[j] = x->btn_key[pos];
				c->btn_value[j] = x->btn_value[pos];
				c->btn_value1[j] = x->btn_value1[pos];
				j++;

				// Shift left the values after pos of x node
				leftShiftVal(x, pos+1, pos, x->btn_count - pos - 1);
				leftShiftPtrs(x, pos+2, pos+1, x->btn_count - pos - 1);
				x->btn_count--;
				
				// Copy cr node values into c
				copyVal(cr, c, 0, j, cr->btn_count);
				copyPtrs(cr, c, 0, j, cr->btn_count+1);
				c->btn_count += (1 + cr->btn_count);

				// Write modified node x
				oldcno = cno;
				writeModifiedRoot(x, xno, c, &cno);

				// Free cl node by writing zeroed node b3
				freeNode(crno);
				releaseNode(crno, CACHE_ENTRY_DIRTY);
				if (clno != 0) 
					releaseNode(clno, 0);

				// Delete key from c
				if (cno != 1) {
					putNode(cno, c);
					retval = deleteVal1(key, value, value1, c, cno);
					releaseNode(cno, CACHE_ENTRY_DIRTY);
					return retval;
				}
				else {
					releaseNode(oldcno, 0);
					retval = deleteVal1(key, value, value1, &bt_root, 1);
					return retval;
				}
			}
			else {
				// Merge with left
				// First shift all the fields of c to the 
				// right most position.
				j = IIHMAXKEYS-1;
				rightShiftVal(c, c->btn_count-1, j, c->btn_count);	
				rightShiftPtrs(c, c->btn_count, j+1, c->btn_count+1);	
				j = j - c->btn_count;
				// Copy median value from x.
				c->btn_key[j] = x->btn_key[pos-1];
				c->btn_value[j] = x->btn_value[pos-1];
				c->btn_value1[j] = x->btn_value1[pos-1];
				j--;

				// Shift left the values after pos of x node
				// As there is no right sibling only one val
				// and one pointer need to be shfted.
				x->btn_ptrs[pos-1] = x->btn_ptrs[pos];
				x->btn_count--;
					
				// Copy cl node values into c
				copyVal(cl, c, 0, 0, cl->btn_count);
				copyPtrs(cl, c, 0, 0, cl->btn_count+1);
				c->btn_count += (1 + cl->btn_count);

				// Write modified node x
				oldcno = cno;
				writeModifiedRoot(x, xno, c, &cno);

				// Free cl node by writing zeroed node b3
				freeNode(clno);
				releaseNode(clno, CACHE_ENTRY_DIRTY);
				if (crno != 0) 
					releaseNode(crno, 0);

				// Delete key from c
				if (cno != 1) {
					putNode(cno, c);
					retval = deleteVal1(key, value, value1, c, cno);
					releaseNode(cno, CACHE_ENTRY_DIRTY);
					return retval;
				}
				else {
					releaseNode(oldcno, 0);
					retval = deleteVal1(key, value, value1, &bt_root, 1);
					return retval;
				}
			}
		}
		else {
			retval = deleteVal1(key, value, value1, c, cno);
			releaseNode(cno, CACHE_ENTRY_DIRTY);
			return retval;
		}
	}
}

/* Search range of values */
void IIIBTree::searchRange(unsigned int l, unsigned int *key,
		unsigned int *val, unsigned int *val1, IntIntIntBTNode *node, 
		unsigned int nodeNo) {
	int i;
	unsigned int oldnodeno;

	oldnodeno = nodeNo;
	while (node != NULL ) {
		if (node == NULL) {
			*key = 0;
			*val = 0;
			*val1 = 0;
			return;
		}

		i = 0;

		while ((i < node->btn_count) && 
			(l > (node->btn_key[i] + node->btn_value[i]-1))) {
			i++;
		}	

		// Check if the range overlaps with the current key+value 
		if ((i < node->btn_count) && 
			(((l >= node->btn_key[i]) && 
			 (l <= (node->btn_key[i] + node->btn_value[i]-1))))) {
			// Matching entry found
			*key = node->btn_key[i];
			*val = node->btn_value[i];
			*val1 = node->btn_value1[i];
			if (oldnodeno != nodeNo)
				releaseNode(nodeNo, 0);
			return;
		}
		if (node->btn_leaf) {
			*key = 0;
			*val = 0;
			*val1 = 0;
			if (oldnodeno != nodeNo)
				releaseNode(nodeNo, 0);
			return;
		}
		else {
			if (oldnodeno != nodeNo)
				releaseNode(nodeNo, 0);
			nodeNo = node->btn_ptrs[i];
			if (nodeNo > 0) {
				node = getNode(nodeNo);
			}
			else node = NULL;
		}	
	}

	*key = 0;
	*val = 0;
	*val1 = 0;
	return;
}

/* search key in B-Tree */
void IIIBTree::searchVal(unsigned int key, unsigned int *resNodeNo, int *pos, 
		unsigned int *value, unsigned int *value1, 
		IntIntIntBTNode *node, unsigned int nodeNo) {
	int i;
	unsigned int oldnodeno;

	oldnodeno = nodeNo;
	while (node != NULL ) {
		if (node == NULL) {
			*resNodeNo = 0;
			*pos = -1;
			*value = 0;
			*value1 = 0;
			return;
		}

		i = 0;

		while ((i < node->btn_count) && 
			(key > node->btn_key[i])) {
			i++;
		}	

		// If found in the current node
		if ((i < node->btn_count) && 
			(key ==  node->btn_key[i])) {
			*resNodeNo = nodeNo;	// For debugging
			*pos = i;
			*value = node->btn_value[i];
			*value1 = node->btn_value1[i];
			if (oldnodeno != nodeNo)
				releaseNode(nodeNo, 0);
			return;
		}
		if (node->btn_leaf) {
			*resNodeNo = 0;
			*pos = -1;
			*value = 0;
			*value1 = 0;
			if (oldnodeno != nodeNo)
				releaseNode(nodeNo, 0);
			return;
		}
		else {
			if (oldnodeno != nodeNo)
				releaseNode(nodeNo, 0);
			nodeNo = node->btn_ptrs[i];
			if (nodeNo > 0) {
				node = getNode(nodeNo);
			}
			else node = NULL;
		}	
	}

	*resNodeNo = 0;
	*pos = -1;
	*value = 0;
	*value1 = 0;
	return;
}

int IIIBTree::display(IntIntIntBTNode *x, unsigned int xno, int level) {
	IntIntIntBTNode *b;
	int i;

	displayNode(x, xno, level);
	if (!(x->btn_leaf))
		for (i=0; i<=x->btn_count; i++) {
			b = getNode(x->btn_ptrs[i]);
			display(b, x->btn_ptrs[i], level+1);
			releaseNode(x->btn_ptrs[i], 0);
		}
	return 0;
}

void IIIBTree::enumKeys(IntIntIntBTNode *x, unsigned int xno, int level) {
	IntIntIntBTNode *b;
	int i;

	if (!(x->btn_leaf)) {	
		for (i=0; i<x->btn_count; i++) {
			b = getNode(x->btn_ptrs[i]);
			enumKeys(b, x->btn_ptrs[i], level+1);
			cout << x->btn_key[i] << "\t" << x->btn_value[i]<< "\t" << x->btn_value1[i] << endl;
			releaseNode(x->btn_ptrs[i], 0);
		}

		// Rightmost child
		b = getNode(x->btn_ptrs[i]);
		enumKeys(b, x->btn_ptrs[i], level+1);
		releaseNode(x->btn_ptrs[i], 0);
	}
	else {
		for (i=0; i<x->btn_count; i++) {
			cout << x->btn_key[i] << "\t" << x->btn_value[i]<< "\t" << x->btn_value1[i] << endl;
		}
	}

	return;
}

int IIIBTree::countKeys(IntIntIntBTNode *x, int level) {
	IntIntIntBTNode *b;
	int i;
	int keyCount = 0;

	nodeCount++;
	keyCount += x->btn_count;
	if (maxlevel < level) maxlevel = level;

	if (!(x->btn_leaf))
		for (i=0; i<=x->btn_count; i++) {
			b = getNode(x->btn_ptrs[i]);
			keyCount += countKeys(b, level+1);
			releaseNode(x->btn_ptrs[i], 0);
		}

	if (level == 0) 
		cout << "Node count : " << nodeCount << " key count : " 
			<< keyCount << " maxlevel " << maxlevel << endl;
	return keyCount;
}

int IIIBTree::enumKeysVals(IntIntIntBTNode *x, int level, unsigned int key[], unsigned int val[], unsigned int val1[], unsigned int ind) {
	IntIntIntBTNode *b;
	int i;
	int count, total = 0;

	if (!(x->btn_leaf)) {	
		for (i=0; i<x->btn_count; i++) {
			b = getNode(x->btn_ptrs[i]);
			count = enumKeysVals(b, level+1, key, val, val1, ind);
			key[count+ind] = x->btn_key[i];
			val[count+ind] = x->btn_value[i];
			val1[count+ind] = x->btn_value1[i];
			ind += (count + 1);
			total += (count + 1);
			releaseNode(x->btn_ptrs[i], 0);
		}

		// Rightmost child
		b = getNode(x->btn_ptrs[i]);
		count = enumKeysVals(b, level+1, key, val, val1, ind);
		releaseNode(x->btn_ptrs[i], 0);
		total += count;
	}
	else {
		for (i=0; i<x->btn_count; i++) {
			key[i+ind] = x->btn_key[i];
			val[i+ind] = x->btn_value[i];
			val1[i+ind] = x->btn_value1[i];
		}
		total = x->btn_count;
	}

	return total;
}

void IIIBTree::displayNode(IntIntIntBTNode *x, unsigned int xno, int level) {
	int i;

	cout << "[" << level << ":" << xno << ":" << x->btn_leaf 
		<< ":" << x->btn_count << ":";
	for (i=0; i<x->btn_count; i++) 
		cout << "(" << x->btn_ptrs[i] << "," << x->btn_key[i] << "," << x->btn_value[i] << "," << x->btn_value1[i] << ")";
	cout << "(" << x->btn_ptrs[i] << ")]\n";
}

//=============================

void IIIBTree::kvdelete(unsigned int i, IIIBTVal *c) {
	//cout << " kvdelete i " << i << " key " << c->key << endl;
        if (c->next != NULL) (c->next)->prev = c->prev;
        if (c->prev != NULL) (c->prev)->next = c->next;
        else KVQ[i] = c->next;

        if (c->tnext != NULL) (c->tnext)->tprev = c->tprev;
        else KVTail = c->tprev;
        if (c->tprev != NULL) (c->tprev)->tnext = c->tnext;
        else KVHead = c->tnext;
}

void IIIBTree::kvprepend(unsigned int i, IIIBTVal *c) {
	//cout << " kvprepend i " << i << " key " << c->key << endl;
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

IIIBTVal * IIIBTree::kvsearch(unsigned int i, unsigned int key) {
        IIIBTVal  *c;

        c = KVQ[i];
        while ((c != NULL) && (c->key != key))
                c = c->next;
        return c;
}

IIIBTVal * IIIBTree::kvalloc(void) {
        IIIBTVal  *c = NULL;
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
                        if (c->flags & CACHE_ENTRY_COPY) {
                                updateVal(c->key, c->val, c->val1, &bt_root, 1);
                        }
                        else {
                                insertVal(c->key, c->val, c->val1);
                        }
                }
        }

        return c;
}


int IIIBTree::insertValWrap(unsigned int key, unsigned int val, 
	unsigned int val1) {
	IIIBTVal *c;
	unsigned int i;
	int retval;
#ifdef	__DEBUG__
	//cout << bt_fname << "::ibtree insert key " << key 
		//<< " val " << val << endl;
#endif
	BTINSERT++;
	// Search for the key
	i = key % KVHASH;
	c = kvsearch(i, key);

	if (c != NULL) { 
		// Update the entry
		kvdelete(i, c);

		// Assign new value
		c->val = val;
		c->val1 = val1;
		c->flags |= CACHE_ENTRY_DIRTY;

		kvprepend(i, c);
		retval = 0;
	}
	else {
		// Add a new entry
		c = kvalloc();
		c->key = key;
		c->val = val;
		c->val1 = val1;
		c->flags = CACHE_ENTRY_DIRTY;
		kvprepend(i, c);
		retval = 1;
	}

	return retval;
}

int IIIBTree::deleteValWrap(unsigned int key) {
	IIIBTVal *c;
	unsigned int i;
	int retval = 0;

#ifdef	__DEBUG__
	//cout << bt_fname << "::ibtree delete key " << key << endl;
#endif	
	BTDELETE++;
	i = key % KVHASH;
	c = kvsearch(i, key);

	if ((c == NULL) || (c->flags & CACHE_ENTRY_COPY)) {
		deleteVal(key, 0, 0, &bt_root, 1);
		retval = 1;
	}

	if (c != NULL) {
		kvdelete(i, c);
		c->next = FL;
		c->prev = NULL;
		FL = c;
	}

	return retval;
}

/* Search range of values */
void IIIBTree::searchRangeWrap(unsigned int l, unsigned int *key,
		unsigned int *val, unsigned int *val1) {
	//unsigned int i;
	//IIIBTVal *c;

	BTSEARCH++;
/*
	// Search the cached lists
	for (i=0; i<KVHASH; i++) {
		c = KVQ[i];
		while ((c != NULL) && (! ((c->key <= l) && ((c->key + c->val - 1) >= l))))
			c = c->next;
		if (c != NULL) {
			*key = c->key;
			*val = c->val;
			*val1 = c->val1;
			return;
		}
	}
*/
	// Not found in the cachd lists,
	// search, the btree nodes	
	searchRange(l, key, val, val1, &bt_root, 1);
	return;
}


int IIIBTree::searchValWrap(unsigned int key, unsigned int *val, unsigned int *val1) {
	IIIBTVal *c;
	unsigned int nodeno;
	int pos;
	unsigned int i;
	int retval = 0;

	BTSEARCH++;
	// First search in the cache
	i = key % KVHASH;
	c = kvsearch(i, key);
	if (c != NULL) {
		// Entry exists
		*val = c->val;
		*val1 = c->val1;
		kvdelete(i, c);
		kvprepend(i, c);
		retval = 1;
	}
	else {
		// Search in btree index
		searchVal(key, &nodeno, &pos, val, val1, &bt_root, 1);

		if (nodeno != 0) {
			// Found, add an entry to cache
			c = kvalloc();
			c->key = key;
			c->val = *val;
			c->val1 = *val1;
			c->flags = CACHE_ENTRY_CLEAN | CACHE_ENTRY_COPY;
			kvprepend(i, c);
			retval = 1;
		}
		else {
			*val = 0;
			*val1 = 0;
		}
	}

#ifdef	__DEBUG__
	//cout << bt_fname << "::ibtree search key " << key << " val " << *val << endl;
#endif
	return retval;
}

int IIIBTree::updateValWrap(unsigned int key, unsigned int val, unsigned int val1) {
	IIIBTVal *c; 
	unsigned int i, val11, val12;
	int retval;

#ifdef	__DEBUG__
	//cout << bt_fname << "::ibtree update key " << key << " val " << val << endl;
#endif
	BTUPDATE++;
	//assert((key > 0) && (val > 0));

	// Search if entry is there
	retval = searchValWrap(key, &val11, &val12);

	if (retval != 0) {
		// already exists	
		i = key % KVHASH;
		c = kvsearch(i, key);
		if (c != NULL) {
			if ((val11 != val) || (val12 != val1)) {
				// Update existing entry
				kvdelete(i, c);
				c->val = val;
				c->val1 = val1;
				c->flags |= CACHE_ENTRY_DIRTY;
				kvprepend(i, c);
			}
			// else nothing needs to be done
			retval = 1;
		}
		else {
#ifdef	__DEBUG__
			//cout << bt_fname << "::IIIBTree : updateValWrap entry found\n"
				//<< "But not brought into cache ???\n"
				//<< "So inserting into cache ???\n";
#endif
			insertValWrap(key, val, val1);
			retval = 0;
		}
	}
	else {
#ifdef	__DEBUG__
		//cout << bt_fname << "::IIIBTree : updateValWrap, entry not found\n"
			//<< "But being updated ???\n"
			//<< "So inserting into cache ???\n";
#endif
		insertValWrap(key, val, val1);
		retval = 0;
	}

	return retval;
}

void IIIBTree::flushValWrap(void) {
	IIIBTVal *c;

	c = KVTail;
	while (c != NULL) {
		if (c->flags & CACHE_ENTRY_DIRTY) {
			if (c->flags & CACHE_ENTRY_COPY) {
				updateVal(c->key, c->val, c->val1, &bt_root, 1);
			}
			else {
				insertVal(c->key, c->val, c->val1);
				c->flags |= CACHE_ENTRY_COPY;
			}
			c->flags ^= CACHE_ENTRY_DIRTY;
		}

		c = c->tprev;
	}
}


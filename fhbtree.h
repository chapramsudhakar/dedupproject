#ifndef __FHBTREE_H__
#define __FHBTREE_H__

using namespace std;

//#include "dedupconfig.h"
//#include "dedupn.h"
#include "freelist.h"

#include "darc.h"
#include "lru.h"

#define FHMINKEYS		7
#define FHMAXKEYS 	(2*FHMINKEYS+1)	// Size of the node will be 000 ~ 512
#define FHMINDEGREE	(FHMINKEYS+1)	// Minimum degree
 
struct FileHashBTNode {
	unsigned char 	btn_key[FHMAXKEYS][16]; // Whole file hash Keys
	unsigned int	btn_value[FHMAXKEYS];     // matching block addresses
	unsigned int	btn_value2[FHMAXKEYS];     // Count of blocks
	unsigned int	btn_value3[FHMAXKEYS];     // Reference count
	unsigned int	btn_ptrs[FHMAXKEYS+1];
	unsigned int    btn_ucount;
	// First node's btn_ucount contains total number of
	// used nodes in the entire index file.
	// Ponters to btree nodes (numbers)
	unsigned short  btn_used;       // Is this used node or free node
	unsigned short  btn_leaf;       // Is this leaf node
	unsigned short  btn_count;      // Count of keys
	unsigned char   btn_padding[16];//round to 512 bytes
};

struct FHBTVal {
	unsigned char    key[16];	// Whole file hash value
	unsigned int    val;		
	unsigned int    val2;
	unsigned int    val3;
	struct FHBTVal   *next, *prev;
	struct FHBTVal   *tnext, *tprev;
	unsigned short  flags;
};

class StorageNode;
class FileHASHBTree {
private:
	char		bt_fname[100];	// Name of the index file
	fstream		bt_fs;		// File stream
	unsigned int	bt_nblks;	// Current number of blocks
	unsigned int	bt_maxblks;	// Maximum number of blocks
	unsigned int	bt_incr;
	unsigned int 	bt_hashsize;	// Cache hash list size
#ifdef	DARCP
	DARC		*bt_btcache;	// btree node cache
#else
	LRU		*bt_btcache;	// btree node cache
#endif


	struct FHBTVal   *FL;
	unsigned int    KVCacheSize;
	unsigned int	KVHASH;
	struct FHBTVal   **KVQ;
	struct FHBTVal   *KVTail;
	struct FHBTVal   *KVHead;

	FreeList	*flist;
	unsigned int	nextFreeHBTreeNode;
	int nodeCount;
	int maxlevel;
	StorageNode	*bt_sn;

	// Free node by writing zero bytes in the node
	void freeNode(unsigned int n);

	// Find a one free (unused) node
	unsigned int findFreeNode(void);
	
	// Split the child node into two nodes
	void splitChild(struct FileHashBTNode *par, unsigned int parNo,
			int i, struct FileHashBTNode *child, unsigned int childNo);

	// Insert a key and its address in a non-full Node
	void insertNonFull(struct FileHashBTNode *b, unsigned int bNo, 
			unsigned char * key, unsigned int val,
			unsigned int val2, unsigned int val3);

	// Left shift value and lba elements position 'from' to 'to' 
	void leftShiftVal(struct FileHashBTNode *b, 
			int from, int to, int count);

	// Left shift ptrs position 'from' to 'to' 
	void leftShiftPtrs(struct FileHashBTNode *b, int from, int to, int count);

	// right shift value and lba elements position 'from' to 'to'
	void rightShiftVal(struct FileHashBTNode *b, int from, int to, int count);
	
	// right shift ptrs position 'from' to 'to'
	void rightShiftPtrs(struct FileHashBTNode *b, int from, int to, int count);

	// Copy ptrs from b1 to b2 from position 'from' to 'to'
	void copyPtrs(struct FileHashBTNode *b1, struct FileHashBTNode *b2, 
			int from, int to, int count);

	// Copy val and lba elements from b1 to b2 from position 'from' to 'to'
	void copyVal(struct FileHashBTNode *b1, struct FileHashBTNode *b2, 
			int from, int to, int count);

	// Modified root/root of a subtree, whose child is y, is 
	// to be written with proper checking.
	void writeModifiedRoot(struct FileHashBTNode *x, unsigned xno, 
		struct FileHashBTNode *y, unsigned int * yno);

	void findMin(unsigned char *key, unsigned int *value,
			unsigned int *val2, unsigned int *val3,	
			FileHashBTNode *x, unsigned int xno);

	void findMax(unsigned char *key, unsigned int *value,
			unsigned int *val2, unsigned int *val3,	
			FileHashBTNode *x, unsigned int xno);
	int deleteVal1(unsigned char * key, unsigned int val,
			unsigned int val2, unsigned int val3,	
			struct FileHashBTNode *x, unsigned int xno);

	// Read a node from the index file
	int readNode(unsigned int n, struct FileHashBTNode *b);

	// Write a node into the index file
	int writeNode(unsigned int n, struct FileHashBTNode *b);

	// Returns the block if found in the cache otherwise read the node,
	// and add to cache 
	FileHashBTNode * getNode(unsigned int n);

	// Writes the block, into cached block,
	// otherwise creates a new cached copy
	int putNode(unsigned int n, FileHashBTNode *b);

	// Releases the block, if modified then set the dirty flag
	void releaseNode(unsigned int n, unsigned short dirty);

	void kvdelete(unsigned int i, FHBTVal *c);
	void kvprepend(unsigned int i, FHBTVal *c);
	FHBTVal * kvsearch(unsigned int i, unsigned char *key);
	FHBTVal * kvalloc(void);
	//int insertValWrap(unsigned char * key, unsigned int val);
	//int deleteValWrap(unsigned char * key);
	//int searchValWrap(unsigned char *key, unsigned int *val);
	//void flushValWrap(void);

public:
	unsigned long int keycount;
	////////////////////////////////////////////////////
	unsigned long long int	BT_BTNODE_READCOUNT;
	unsigned long long int	BT_BTNODE_READ_CACHE_HIT;
	unsigned long long int	BT_BTNODE_WRITECOUNT;
	unsigned long long int	BT_BTNODE_WRITE_CACHE_HIT;
	unsigned long long int	BT_BTNODE_EFFECTIVE_WRITES;
	unsigned long long int	BT_BTNODE_FLUSH_WRITES;

	unsigned long long int	BT_BTNODE_READCOUNT_READ;
	unsigned long long int	BT_BTNODE_WRITECOUNT_READ;
	unsigned long long int	BT_BTNODE_READCOUNT_WRITE;
	unsigned long long int	BT_BTNODE_WRITECOUNT_WRITE;
	unsigned long long int    BTSEARCH;
	unsigned long long int    BTUPDATE;
	unsigned long long int    BTINSERT;
	unsigned long long int    BTDELETE;

	///////////////////////////////////////////////////

	struct FileHashBTNode 	bt_root;	// Root node

	// Constructor
	FileHASHBTree(const char fname[100], unsigned int mblocks, 
			unsigned int hashsize, unsigned int incr, StorageNode *sn);  
	

	// Destructor, writes root node and closes the index file
	~FileHASHBTree();

	// Initialize the index file
	void initialize(void);

	// Search for a value in the tree under node
	void searchVal(unsigned char * key, unsigned int *resNodeNo, 
			int *pos, unsigned int *val,
			unsigned int *val2, unsigned int *val3,	
			struct FileHashBTNode *node, unsigned int nodeNo);

	// Insert a key and its address
	void insertVal(unsigned char * key, unsigned int val,
			unsigned int val2, unsigned int val3);

	void updateVal(unsigned char *key, unsigned int val,
			unsigned int val2, unsigned int val3,	
			struct FileHashBTNode * node, unsigned int nodeNo);

	// Update the value of an exisiting key in the node at the 
	// specified index position
	void updateVal(unsigned char *key, unsigned int nodeNo, 
			unsigned int pos, unsigned int val,
			unsigned int val2, unsigned int val3);

	// Delete a value from the tree under node x
	int deleteVal(unsigned char * key, unsigned int val,
			unsigned int val2, unsigned int val3, 
			struct FileHashBTNode *x, unsigned int xno);
	
	// Display the entire btree
	int display(struct FileHashBTNode *x, unsigned int xno, int level);

	// Display the one node of btree
	void displayNode(struct FileHashBTNode *x, unsigned int xno, int level);
	void enableCache(unsigned int size);
	void enableKeyValCache(unsigned int size);

	// Cache flush operation
	void flushCache(int disk);
	
	void enumKeys(FileHashBTNode *x, unsigned int xno, int level);
	int countKeys(struct FileHashBTNode *x, int level);
	void displayStatistics(void);

	// Wrapper routines
	int insertValWrap(unsigned char * key, unsigned int val,
			unsigned int val2, unsigned int val3);
	int updateValWrap(unsigned char * key, unsigned int val,
			unsigned int val2, unsigned int val3);
	int incrementRefCountWrap(unsigned char *key);
	int decrementRefCountWrap(unsigned char *key);
	int searchValWrap(unsigned char * key, unsigned int *val,
			unsigned int *val2, unsigned int *val3);
	int deleteValWrap(unsigned char * key);
	void flushValWrap(void);
};

#endif

#ifndef __GRAPH_H__
#define __GRAPH_H__

using namespace std;

#define	GRAPH_HASH_SIZE	500000

struct GrNode {
	struct GrNode * gn_next;	// Next node in the adjcency list.
	struct GrNode * gn_ident_next;	// All list of identical nodes
	unsigned int 	gn_v;		// starting block no
	unsigned short 	gn_count;	// Count of blocks
};

struct GrHNode {	// Graph Header node
	unsigned long long int 	ghn_ts;	// Last access time stamp
	struct GrNode *	ghn_adj;	// Adjacency list
	struct GrNode * ghn_ident_list;	// This nodes identical list
	struct GrHNode*	ghn_next;	// Next header node
	unsigned int 	ghn_v;		// starting block no
	unsigned int	ghn_rdcount;	// Read count of this sequence
	unsigned int	ghn_wrcount;	// write count of this sequence
	//unsigned int	ghn_pba;	// Starting pba number
	unsigned short 	ghn_count;	// Count of blocks
};

struct HashNode {
	struct GrHNode*		hn_grhdr;	
			// Corresponding Header node in graph 
	struct HashNode* 	hn_next;
	struct HashNode* 	hn_prev;
			// Node is maintained in a hash list for searching
	unsigned int 		hn_v;		// block no
};


class Graph {
private:
	struct GrHNode * 	gr_hdr; // Graph, adjacency list header node 
	struct HashNode **	gr_hash_list;
					// Individual nodes (block no) hash list
	unsigned long		gr_count;	// Graph node count
	unsigned long		gr_hcount; 	// Nodes in the hash.
	unsigned long 		gr_ecount;	// Edge count

	// Get the hash node corresponding to the block v
	struct HashNode * getHashNode(unsigned int v);

	// Split the node value ranging [v to v+count] into
	// [v, v2-1] and [v2, ...]. Adjacency list of old node remains as
	// adjacency list of [v2, ...]. Node [v, v1-1] is returned.
	struct GrHNode * splitNode(struct GrHNode * onode, unsigned int v2);

	// Add a new segment (node) whose blocks are not existing 
	// already in the graph.
	void addNewNode(unsigned int v, unsigned short count, 
			unsigned int ts, int rdwr);

public:
	Graph();

	// Add a segment to the graph.
	// Search the hash list for any sub segments already present in 
	// in the graph. If necessary split the exisiting nodes(segments).
	void addNode(unsigned int v, unsigned short count, 
			unsigned long long int ts, int rdwr);

	// Add an edge between the nodes with last block v1 and first block v2
	void addEdge(unsigned int v1, unsigned int v2);

	// Search for exact matching segment
	struct GrHNode * searchNode(unsigned int v, unsigned short count);

	// Search for if a block v is present in a segment (node)
	struct GrHNode * searchNode(unsigned int v);

	void display();
};

#define OP_READ         1
#define OP_WRITE        2

#endif

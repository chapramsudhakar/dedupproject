#include<iostream>
#include<cstdlib>
#include <assert.h>

#include "graph.h"

using namespace std;

// Get the hash node corresponding to the block v
struct HashNode * Graph::getHashNode(unsigned int v) {
	struct HashNode *hn;

	// Get the hash list of the block v
	hn = gr_hash_list[v % GRAPH_HASH_SIZE];

	// Perform linear serach in the list
	while (hn != NULL) {
		if (hn->hn_v == v) return hn;
		hn = hn->hn_next;
	}

	// If v is not found?
	return NULL;
}
	

// Split the node value ranging [v to v+count] into
// [v, v2-1] and [v2, ...]. Adjacency list of old node remains as
// adjacency list of [v2, ...]. Node [v2, ...] is returned.
struct GrHNode * Graph::splitNode(struct GrHNode * onode, unsigned int v2) {
	struct GrHNode *nnode;	// New node
	struct GrNode *agn;	// Adjacency node
	struct HashNode *hn1, *hn2;
	struct GrNode *ignold, *ign1, *ign2;	// Identical graph node lists
	unsigned short count1, count2;
	int i;

	//cout << "Split node : " << onode->ghn_v << " " << v2 << endl;
	// Create a new header node
	nnode = (struct GrHNode *)malloc(sizeof(struct GrHNode));
	assert(nnode != NULL);
	gr_count++;
	nnode->ghn_v = v2;

	nnode->ghn_count = count2 = onode->ghn_count - (v2 - onode->ghn_v);
	onode->ghn_count = count1 = v2 - onode->ghn_v;

	nnode->ghn_rdcount = onode->ghn_rdcount;
	nnode->ghn_wrcount = onode->ghn_wrcount;
	nnode->ghn_ts = onode->ghn_ts;

	nnode->ghn_adj = onode->ghn_adj;
	agn = (struct GrNode *)malloc(sizeof(struct GrNode));
	assert(agn != NULL);
	gr_ecount++;
	agn->gn_v = v2;
	agn->gn_count = nnode->ghn_count;
	agn->gn_next = NULL;
	onode->ghn_adj = agn;
	agn->gn_ident_next = NULL;
	nnode->ghn_ident_list = agn;

	// Add this new header node at the beginning
	nnode->ghn_next = gr_hdr;
	gr_hdr = nnode;

	// All old identical nodes must be updated
	ignold = onode->ghn_ident_list;
	
	while (ignold != NULL) {
		// Change the block count of old node
		ignold->gn_count = count1;

		// Go to process the next node
		ignold = ignold->gn_ident_next;
	}

	// Change the hash node pointers for the blocks in the range v2 ...
	for (i=0; i<count2; i++) 
		getHashNode(v2+i)->hn_grhdr = nnode;

	return nnode;	
}

// Add a new segment (node) whose blocks are not existing 
// already in the graph.
void Graph::addNewNode(unsigned int v, unsigned short count, 
		unsigned int ts, int rdwr) {
	struct GrHNode *gn;
	struct HashNode *hn1, *hn2;
	unsigned short i;

	//cout << "New node being added : " << v << " " << count << endl;
	// Allocate memory of the node and initialize
	// all fields.
	gn = (struct GrHNode *)malloc(sizeof(struct GrHNode));
	assert (gn != NULL);
	gr_count++;
	gn->ghn_v = v;
	gn->ghn_count = count;
	gn->ghn_ts = ts;
	if (rdwr == OP_READ)  {
		gn->ghn_rdcount = 1;
		gn->ghn_wrcount = 0;
	}
	else {
		gn->ghn_rdcount = 0;
		gn->ghn_wrcount = 1;
	}
	gn->ghn_adj = NULL;
	gn->ghn_ident_list = NULL;
		
	// Add this node at the beginning iof the header node list
	gn->ghn_next = gr_hdr;
	gr_hdr = gn;

	// create all corresponding individual blocks hash nodes
	for (i=0; i<count; i++) {
		hn1 = (struct HashNode *)malloc(sizeof(struct HashNode));
		assert (hn1 != NULL);
		gr_hcount++;
		hn1->hn_v = v + i;
		hn1->hn_grhdr = gn;
		hn1->hn_prev = NULL;
		hn2 = gr_hash_list[(v+i) % GRAPH_HASH_SIZE];
		hn1->hn_next = hn2;
		if (hn2 != NULL) hn2->hn_prev = hn1;
		gr_hash_list[(v+i) % GRAPH_HASH_SIZE] = hn1;
	}
}

Graph::Graph() {
	int i;

	gr_hdr = NULL;
	gr_count = 0;
	gr_hcount = 0;
	gr_ecount = 0;

	for (i=0; i<GRAPH_HASH_SIZE; i++)
		gr_hash_list[i] = NULL;
}

// Add a segment to the graph.
// Search the hash list for any sub segments already present in 
// in the graph. If necessary split the exisiting nodes(segments).
void Graph::addNode(unsigned int v, unsigned short count, 
	unsigned long long int ts, int rdwr) {
	struct GrHNode *gn, *gn1, *gn2;
	unsigned short i;
	unsigned int v1, v2, v3;
	unsigned short count1, count2, count3;

	//cout << "Add node : " << v << " " << count << endl;
	// Check whether exact identical node already exists
	if ((gn = searchNode(v, count)) != NULL) {
		// Update access time and read/write count
		gn->ghn_ts = ts;
		if (rdwr == OP_READ) gn->ghn_rdcount++;
		else gn->ghn_wrcount++;
		return;
	}

	// Search if any partial node match is there
	for (i=0; i<count; i++) {
		if ((gn = searchNode(v + i)) != NULL) break;
	}
		
	if (gn != NULL) { // Partial node match found
		v1 = v + i;	// Starting matching node
		count1 = count - i;
		if (v1 != v) {
			// There is some prefix segment that is not 
			// yet added to the graph.
			// Add v to v1-1 as a new segment
			addNewNode(v, i, ts, rdwr);
		}
			
		// Find the matching block count from v1 onwards
		if (v1 == gn->ghn_v) { // at the beginning
			if (count1 < gn->ghn_count) {
				// starting from v1 entirely this new
				// segment is available in the 
				// old segment. So split the 
				// old segment
				gn1 = splitNode(gn, v1 + count1); 

				// Change the new node time stamp
				// and read/write count
				gn1->ghn_ts = ts;
				if (rdwr == OP_READ) gn1->ghn_rdcount++;
				else gn1->ghn_wrcount++;
			}
			else if (count1 == gn->ghn_count) {
				// Exactly equal
				// Change the existing node time stamp
				// and read/write count
				gn->ghn_ts = ts;
				if (rdwr == OP_READ) gn->ghn_rdcount++;
				else gn->ghn_wrcount++;
			}
			else {
				// New segment is longer than 
				// existing segment.
				// Change the existing node time stamp
				// and read/write count
				gn->ghn_ts = ts;
				if (rdwr == OP_READ) gn->ghn_rdcount++;
				else gn->ghn_wrcount++;

				// Compute the remaining segment
				// and follow the same steps from the
				// beginning.
				v2 = v1 + gn->ghn_count;
				count2 = count1 - gn->ghn_count;
				addNode(v2, count2, ts, rdwr);
			}
		}
		else if (v1 > gn->ghn_v) {
			// Segment matching in the middle 
			// Split the old node 
			// and call addNode recursively 
			// so that it will be handled by 
			// the previous cases.
			splitNode(gn, v1);
			addNode(v1, count1, ts, rdwr);
		}
	}
	else {
		// New segment is an entirely new node
		addNewNode(v, count, ts, rdwr);
	}
}

// Add an edge between the nodes with last block v1 and first block v2
void Graph::addEdge(unsigned int v1, unsigned int v2) {
	GrHNode *gn1, *gn2;	// Header nodes
	GrNode *agn;		// Adjacency node
	
	// Assuming that already Header nodes corresponding to the 
	// segment ending with v1 and segment starting with v2 are
	// added. Now adjcency node is to be created. 
	// (If not existing already.)
	gn1 = searchNode(v1);
	gn2 = searchNode(v2);

	//cout << "Add edge : " << v1 << " " << v2 << endl;
	if ((gn1 == NULL) || (gn2 == NULL)) {
		cout << "Error: Header node should be added before adding an edge\n";
		exit(0);
	}
		
	// Check if already edge exists?
	agn = gn1->ghn_adj;

	while (agn != NULL) {
		if (agn->gn_v == v2) // Edge present
			break;
		agn = agn->gn_next;
	}

	if (agn == NULL) { // Edge not present
		// Allocate and initialize adjcency node
		agn = (struct GrNode *) malloc(sizeof(struct GrNode));
		assert(agn != NULL);
		gr_ecount++;
		agn->gn_v = v2;
		agn->gn_count = gn2->ghn_count;

		// Add to the adjacency list of gn1 at the beginning
		agn->gn_next = gn1->ghn_adj;
		gn1->ghn_adj = agn;
		
		// Add to the gn2 header node's identical list of 
		// adjacency nodes at the beginning
		agn->gn_ident_next = gn2->ghn_ident_list;
		gn2->ghn_ident_list = agn;
	}

	return;
}

// Search for exact matching segment
struct GrHNode * Graph::searchNode(unsigned int v, unsigned short count) {
	struct GrHNode * gn;

	// Search for starting block	
	gn = searchNode(v);

	if (gn != NULL) { // Found	
		// Find whether the segment returned is 
		// exactly matching?
		if (gn->ghn_count == count) return gn;
	}

	// Exact matching node not found
	return NULL;
}

// Search for if a block v is present in a segment (node)
struct GrHNode * Graph::searchNode(unsigned int v) {
	struct HashNode * hn;

	// Get the hash list of the block v
	hn = gr_hash_list[v % GRAPH_HASH_SIZE];

	// Perform lninear serach in the list
	while (hn != NULL) {
		if (hn->hn_v == v) return hn->hn_grhdr;
		hn = hn->hn_next;
	}

	// If v is not found?
	return NULL;
}

void Graph::display() {
	struct GrHNode *hdr;
	struct GrNode *grn;
	struct HashNode *hn;
	int i;

	// Display adjacency lists
	hdr = gr_hdr;

	while (hdr != NULL) {
		// Display header node 
		cout << "<" << hdr->ghn_v << ", " << hdr->ghn_count << ", RD="
			<< hdr->ghn_rdcount << ", WR=" << hdr->ghn_wrcount 
			<< ", TS=" << hdr->ghn_ts << ">\n";
		cout <<"Adj nodes: ";
		grn = hdr->ghn_adj;
		while (grn != NULL) {
			cout << "<" << grn->gn_v << ", " << grn->gn_count << ">, ";
			grn = grn->gn_next;
		}
		cout << endl;
		hdr = hdr->ghn_next;
	}

	// Display hash lists	
	for (i=0; i<GRAPH_HASH_SIZE; i++)
	{
		hn = gr_hash_list[i];
		cout << "Hash List [ " << i << " ] = ";
		while (hn != NULL) {
			hdr = hn->hn_grhdr;
			cout << "<" << hdr->ghn_v << ", " << hdr->ghn_count << ">";
			hn = hn->hn_next;
		}
		cout << endl;
	}

	cout << "Graph node count : " << gr_count << endl
		<< "Graph edge count : " << gr_ecount << endl
		<< "Graph hash node count : " << gr_hcount << endl << endl;
}


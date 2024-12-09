#include <iostream>
#include <fstream>
//#include "dedupconfig.h"
//#include "dedupn.h"
#include "dupset.h"

#define VERT_HASH_SIZE	20000

struct Vertex * vlist[VERT_HASH_SIZE];

extern unsigned long long int VERTEX_CREATE_COUNT;
extern unsigned long long int VERTEX_PARENT_CHANGE_COUNT;
extern unsigned long long int VERTEX_FIND_COUNT;
extern unsigned long long int VERTEX_REMOVE_COUNT;

struct Vertex * addVertex(unsigned int lba, unsigned int par) {
	struct Vertex *v, *p;
	int i;

	i = (lba) % VERT_HASH_SIZE;

	v = (struct Vertex*)malloc(sizeof(struct Vertex));
	v->lba = lba;
	v->par = par;
	VERTEX_CREATE_COUNT++;
	
	// Add this into the hash list at the beginning
	v->hprev = NULL;
	v->hnext =  vlist[i];
	if (vlist[i] != NULL) {
		vlist[i]->hprev = v;
	}
	vlist[i] = v;

	// Add this node to the child or siblings list
	if (par == 0) // This itself is the representative vertex
		v->csptr = NULL;
	else {
		p = findVertex(par);
		// If parent vertex set does not exist?
		if (p == NULL) {
			cout << "Parent vertex set " << par << " does not exists!\n";
			p = addVertex(par, 0);
		}
		v->csptr = p->csptr;
		p->csptr = v;
	}

	return v;
}

void setParentVertex(struct Vertex *v, unsigned int par) {
	// Vertex v is already in the hash list
	// It should be added to the child list of par vertex
	struct Vertex *p;

	if (par == 0) {
		v->csptr = NULL;
		return;
	}
	
	p = findVertex(par);
	if (p != NULL) {
		v->par = par;
		v->csptr = p->csptr;
		p->csptr = v;
		VERTEX_PARENT_CHANGE_COUNT++;
	}
	else {
		// If parent vertex set does not exist?
		p = addVertex(par, 0);
		v->csptr = NULL;
		p->csptr = v;
	}
}


struct Vertex *deleteVertex(unsigned int lba, int *empty) {
	struct Vertex *v, *p, *pred, *cur;

	v = findVertex(lba);
	*empty = 0;
	
	if (v->par != 0) {
		// Not a representative vertex of the set
		// Remove it from the siblings list
		p = findVertex(v->par);
		if (p->csptr == v)  {
			// First child node of the set
			p->csptr = v->csptr;
		}
		else {
			pred = p->csptr;
			cur = pred->csptr;

			while ((cur != v) && (cur != NULL)) {
				pred = cur;
				cur = pred->csptr;
			}
			if (cur == NULL) {
				// cur == NULL should never happen, before
				// that required node should be found
				cout << "Error: node " << lba << " not found in the set" << v->par << endl;
			}
			else {
				pred->csptr = cur->csptr;
			}
		}
	}
	else {
		// This vertex is representative vertex of the set
		// Make the first child (if exists) as the representative
		if (v->csptr != NULL)  {
			cout << lba << "Trying to remove representative vertex! But the set is not empty\n";
		}
		else *empty = 1;
	}
	VERTEX_REMOVE_COUNT++;
	return v;
}

struct Vertex *findVertex(unsigned int lba) {
	struct Vertex *v; //, *v1;
	int i;

	i = (lba) % VERT_HASH_SIZE;

	v = vlist[i];

	VERTEX_FIND_COUNT++;

	while (v != NULL) {
		if (v->lba == lba) {
				return v;
		}
		else {
			v = v->hnext;
		}
	}

	return NULL;
}

void displayDupSet(void) {
	struct Vertex *v;
	int i;
	unsigned long long count = 0, lbacount = 0;

	for (i=0; i<VERT_HASH_SIZE; i++) {
		v = vlist[i];

		while (v != NULL) {
			if (v->par == 0) { 
				// Final representative 
				count++;
				lbacount++;
			}
			else {
				lbacount++;
			}
			v = v->hnext;
		}
	}

	cout << "Total number of unique blocks = " << count << endl
		<< "Total lbas  = " << lbacount << endl;
}

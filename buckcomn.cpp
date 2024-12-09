#include <iostream>
using namespace std;

#include "buckcomn.h"

struct pbaList * insertPlNode(struct pbaList *lh, 
		struct pbaList *node) {
	struct pbaList *pred, *cur;

	// Find suitable position
	pred = NULL;
	cur = lh;
	while ((cur != NULL) && (cur->pba < node->pba)) {
		pred = cur;
		cur = cur->next;
	}

	// Insert after predecessor
	node->next = cur;
	if (pred == NULL) return node;
	else {
		pred->next = node;
		return lh;
	}
}


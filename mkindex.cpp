
#include<iostream>
#include<fstream>
#include<cstdlib>
#include<cstring>

#include"ibtree.h"
#include"hbtree.h"
#include"fhbtree.h"
#include"filerecipe.h"

using namespace std;
 
fstream	fs2;
fstream	fs3;
fstream	fs4;
fstream	fs5;
fstream	fs6;
fstream	fs7;


void initializeHashBT(fstream & fs, unsigned int count);
void initializeINTBT(fstream & fs, unsigned int count);
void initializeFileHashBT(fstream & fs, unsigned int count);
void initializeINTINTINTBT(fstream & fs, unsigned int count);

int main() {
	string fn2 = "bucketidx-1.dat";	// Integer index
	string fn3 = "blockidx.dat";	// Integer index
	string fn4 = "hashidx.dat";	// Hash index
	string fn5 = "pbaidx.dat";	// Integer index
	string fn6 = "filehashidx.dat"; // File hash index 
	string fn7 = "filercpidx.dat";	// File recipe index 

	cout << "size of bt node " << sizeof(HashBTNode) << ", " << sizeof(INTBTNode) << endl;

	cout << "Creating lba btree index\n";
	fs3.open(fn3.c_str(), ios::out | ios::binary);
	if (fs3.fail()) {
		cout << "Index file opening failure : " << fn3 << endl;
		exit(0);
	}

	initializeINTBT(fs3, 4096);
	fs3.close();
	cout << "Creating hash btree index\n";
	fs4.open(fn4.c_str(), ios::out | ios::binary);
	if (fs4.fail()) {
		cout << "Index file opening failure : " << fn4 << endl;
		exit(0);
	}

	initializeHashBT(fs4, 4096);
	fs4.close();

	cout << "Creating file hash btree index\n";
	fs6.open(fn6.c_str(), ios::out | ios::binary);
	if (fs6.fail()) {
		cout << "Index file opening failure : " << fn6 << endl;
		exit(0);
	}

	initializeFileHashBT(fs6, 4096);
	fs6.close();

	cout << "Creating file recipe btree index\n";
	fs7.open(fn7.c_str(), ios::out | ios::binary);
	if (fs7.fail()) {
		cout << "Index file opening failure : " << fn7 << endl;
		exit(0);
	}

	initializeINTINTINTBT(fs7, 4096);
	fs7.close();

	return 0;	// Success
}

// Initialize the Hash index file
void initializeHashBT(fstream & fs, unsigned int count) {
	// Only once for the first time running of the program 
	// this should be called.
	unsigned int i;
	HashBTNode b;

	cout << " initialize Hash BTree \n";
	memset((void *)&b, 0, sizeof(HashBTNode));
	b.btn_leaf = 1;
	b.btn_ucount = 1; 
	b.btn_used = 1;
	fs.seekp(0);
	
	fs.write((char *)&b, sizeof(HashBTNode));
       	if (fs.fail()) { 
		// Failed
		cout << " Initialize failed : Written " 
			<< fs.tellp() << endl;
		exit(0);
	}

	b.btn_ucount = 0; 
	b.btn_used = 0;
	fs.clear();
	for (i=2; i<=count; i++) {
		fs.write((char *)&b, sizeof(HashBTNode));
		if (fs.fail()) { 
			// Failed
			cout << " Initialize failed : Written " 
				<< fs.tellp() << endl;
			exit(0);
		}
	}
	
	cout << (i-1) << " number of nodes are added\n";
}

// Initialize the Hash index file
void initializeFileHashBT(fstream & fs, unsigned int count) {
	// Only once for the first time running of the program 
	// this should be called.
	unsigned int i;
	FileHashBTNode b;

	cout << " initialize File Hash BTree \n";
	memset((void *)&b, 0, sizeof(FileHashBTNode));
	b.btn_leaf = 1;
	b.btn_ucount = 1; 
	b.btn_used = 1;
	fs.seekp(0);
	
	fs.write((char *)&b, sizeof(HashBTNode));
       	if (fs.fail()) { 
		// Failed
		cout << " Initialize failed : Written " 
			<< fs.tellp() << endl;
		exit(0);
	}

	b.btn_ucount = 0; 
	b.btn_used = 0;
	fs.clear();
	for (i=2; i<=count; i++) {
		fs.write((char *)&b, sizeof(HashBTNode));
		if (fs.fail()) { 
			// Failed
			cout << " Initialize failed : Written " 
				<< fs.tellp() << endl;
			exit(0);
		}
	}
	
	cout << (i-1) << " number of nodes are added\n";
}

// Initialize the Int index file
void initializeINTBT(fstream & fs, unsigned int count) {
	// Only once for the first time running of the program 
	// this should be called.
	unsigned int i;
	INTBTNode b;

	cout << " initialize Int BTree \n";
	memset((void *)&b, 0, sizeof(INTBTNode));
	b.btn_leaf = 1;
	b.btn_ucount = 1; 
	b.btn_used = 1;
	fs.seekp(0);
	
	fs.write((char *)&b, sizeof(INTBTNode));
       	if (fs.fail()) { 
		// Failed
		cout << " Initialize failed : Written " 
			<< fs.tellp() << endl;
		exit(0);
	}

	b.btn_ucount = 0; 
	b.btn_used = 0;
	fs.clear();
	for (i=2; i<=count; i++) {
		fs.write((char *)&b, sizeof(INTBTNode));
		if (fs.fail()) { 
			// Failed
			cout << " Initialize failed : Written " 
				<< fs.tellp() << endl;
			exit(0);
		}
	}
	
	cout << (i-1) << " number of nodes are added\n";
}

void initializeINTINTINTBT(fstream & fs, unsigned int count) {
	// Only once for the first time running of the program 
	// this should be called.
	unsigned int i;
	IntIntIntBTNode b;

	cout << " initialize Int BTree \n";
	memset((void *)&b, 0, sizeof(IntIntIntBTNode));
	b.btn_leaf = 1;
	b.btn_ucount = 1; 
	b.btn_used = 1;
	fs.seekp(0);
	
	fs.write((char *)&b, sizeof(IntIntIntBTNode));
       	if (fs.fail()) { 
		// Failed
		cout << " Initialize failed : Written " 
			<< fs.tellp() << endl;
		exit(0);
	}

	b.btn_ucount = 0; 
	b.btn_used = 0;
	fs.clear();
	for (i=2; i<=count; i++) {
		fs.write((char *)&b, sizeof(IntIntIntBTNode));
		if (fs.fail()) { 
			// Failed
			cout << " Initialize failed : Written " 
				<< fs.tellp() << endl;
			exit(0);
		}
	}
	
	cout << (i-1) << " number of nodes are added\n";
}


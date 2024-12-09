#include<iostream>
#include<fstream>
#include<iomanip>
#include<cstring>
#include "bucket.h"
#include "hbucket.h"

using namespace std;
struct bucket b;
struct hashbucket hb;

int main() {
	string fname="buckets.dat";
	string fname1 = "hashbuckets.dat";
	ofstream of, of1;
	int i;

	cout << "bucket : " << sizeof(struct bucket) << endl;
	cout << "hash bucket : " << sizeof(struct hashbucket) << endl;

	of.open(fname.c_str(), ios::out | ios::binary | ios::trunc);
	if (of.fail()) {
		cout << "Buckets file opening failure : " << fname << endl;
		exit(0);
	}
	of.clear();

	memset(&b, 0, sizeof(struct bucket));
	cout << "Bucket = " << sizeof(b) << endl;
	//for (i=0; i<MAX_BUCKETS; i++) {
	// 0'th (first) bkt contains max bkt count, used bkt count 
	// and remains in used state.
	b.b_num = 1;
	b.b_nextbucket = 4096;	// Maximum buckets count
	b.b_prevbucket = 1;	// Current used count, 
				// (1 is always marked as used)
	b.b_padding[0] = 1;
	of.write((char *)&b, sizeof(b));
	memset(&b, 0, sizeof(struct bucket));
	for (i=1; i<4096; i++) {
		b.b_num = i+1;
		of.write((char *)&b, sizeof(b));
	}
	of.close();

	of1.open(fname1.c_str(), ios::out | ios::binary | ios::trunc);
	if (of1.fail()) {
		cout << "Hash Buckets file opening failure : " << fname1 << endl;
		exit(0);
	}
	of1.clear();

	memset(&hb, 0, sizeof(struct hashbucket));

	//for (i=0; i<MAX_HBUCKETS; i++) {
	for (i=0; i<HASH_BUCK_COUNT + 16; i++) {
		hb.hb_num = i+1;
		of1.write((char *)&hb, sizeof(hb));
	}
	of1.close();
}



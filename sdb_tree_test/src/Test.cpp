#include <cute_base.h>
#include <cute_runner.h>
#include <cute_suite.h>
#include <cute_test.h>
#include <ide_listener.h>

#include <cstdlib>
#include <iostream>
#include <vector>

#include "storage/datafile.h"
#include "tree/bptree.h"
#include "tree/avl.h"

using namespace std;
using namespace sdb::tree;
using namespace sdb::storage;

template<class K, class V>
class echo_handler: public node_handler<K, V> {
	std::vector<std::string> event_names;
public:

	echo_handler() {
		event_names.push_back("not_found");
		event_names.push_back("equal_event");
		event_names.push_back("greater_event");
		event_names.push_back("less_event");
		event_names.push_back("add_left_event");
		event_names.push_back("add_right_event");
		event_names.push_back("overwrite_event");
		event_names.push_back("rotate_left_event");
		event_names.push_back("rotate_right_event");
	}

	void handle(const sdb::tree::node<K, V> * n) {
		cout << "node k: " << n->k << endl;
	}

	virtual void handle(const node<K, V> *p, const node<K, V> * n,
			node_handler_event e) {
		std::cout << "node n: " << n->k << "; node event: " << event_names[e]
				<< std::endl;
	}

	virtual void handle(const node<K, V> *p, const K k, node_handler_event e) {
		std::cout << "node k: " << p->k << "; const k:" << k << "; node event: "
				<< event_names[e] << std::endl;
	}
};

void test_avl() {

	using namespace sdb::tree;
	sdb::tree::node<int, int> n1, n2, n3, n4, n5, n6;
	n1.k = 1, n2.k = 2, n3.k = 3, n4.k = 4, n5.k = 5, n6.k = 6;

	sdb::tree::avl<int, int> tree(&n1);
	ASSERT(tree.exists(n1.k));

	ASSERT(tree.insert_node(&n2));
	ASSERT(tree.insert_node(&n3));
	ASSERT(tree.insert_node(&n4));
	ASSERT(tree.insert_node(&n5));

	int bf = tree.balance_factor();
	ASSERT(bf < 2 && bf > -2);

	ASSERT(tree.exists(4));
	tree.insert_node(&n6);

	echo_handler<int, int> handler;

	cout << "traverse in order" << endl;
	tree.traverse(&handler, sdb::tree::in_order);

	cout << "traverse pre order" << endl;
	tree.traverse(&handler, sdb::tree::pre_order);

	cout << "traverse post order" << endl;
	tree.traverse(&handler, sdb::tree::post_order);

	cout << "traverse level order" << endl;
	tree.traverse(&handler, sdb::tree::level_order);

	node<int, int> *d = tree.remove(2);
	ASSERT(d != nullptr && d->k == 2);

	cout << "== traverse in order after delete 2 ====" << endl;
	tree.traverse(&handler, sdb::tree::in_order);

	for (int i = 0; i < 20; i++) {
		sdb::tree::node<int, int> n;
		n.k = std::abs(std::rand());
		std::cout << "rand:" << n.k << std::endl;
		tree.insert_node(&n);
	}

	cout << "traverse in order for big tree" << endl;
	tree.traverse(&handler, sdb::tree::in_order);
}

void tree_node_test() {
	sdb::tree::node<int, int> n1, n2, n3, n4, n5;
	n1.k = 1, n1.parent = &n2;
	n2.left = &n1;  // n2 is the root
	n2.k = 2;
	n3.k = 3, n3.parent = &n2, n2.right = &n3;
	n4.k = 4, n4.parent = &n3, n3.right = &n4;
	n5.k = 5, n5.parent = &n4, n4.right = &n5;

	ASSERT(n2.is_root());
	ASSERT(n1.is_left());
	ASSERT(n3.is_right());

	int h = n2.height();
	ASSERT(h == 4);
	ASSERT(n2.stage() == 5);
	ASSERT(n2.left->height() == 1);
	ASSERT(n2.right->height() == 3);

	int bf = n2.balance_factor();

	ASSERT(bf == -2);
	ASSERT(n2.exists(2));
	ASSERT(n2.exists(1));
	ASSERT(n2.exists(4));

	n3.rotate_left();
	ASSERT(n3.exists(4)); // the n3 is the new root
	ASSERT(n2.balance_factor() == 1);
	ASSERT(n3.balance_factor() == 0);

	n2.rotate_right();
	ASSERT(n1.parent == &n2);
	ASSERT(n2.is_root());
	ASSERT(n2.right == &n3);

}

void basic_test_bptree() {
	data_file df(1L, "/tmp/btpree.db");
	if (df.exist()) {
		df.remove();
	}
	df.open();

	index_segment root_seg(1L, &df, 0);
	root_seg.set_length(M_64);

	df.assgin_segment(root_seg);
	df.close();

	ASSERT(root_seg.get_seg_type() == sdb::storage::index_segment_type);
	index_segment r_seg = std::move(root_seg);

	ASSERT(r_seg.get_length() == root_seg.get_length());
	ASSERT(r_seg.get_seg_type() == sdb::storage::index_segment_type);
	ASSERT(df.open() == sdb::SUCCESS);

	ASSERT(df.fetch_segment(r_seg) == sdb::SUCCESS);
	ASSERT(r_seg.has_buffer());
	ASSERT(r_seg == root_seg);

	ASSERT(root_seg.has_buffer() == false);
	bptree tree(&root_seg, 8, 8, K_4);
}

void basic_page_test() {
	data_file df(1L, "/tmp/btpree.db");
	if (df.exist()) {
		df.remove();
	}
	df.open();

	index_segment seg(1L, &df, 0);
	seg.set_length(M_64);
	seg.set_block_size(K_4);
	seg.assign_content_buffer();

	// before assign a index page, must know the key length and value length,
	// whereas create a bptree object
	bptree tree(&seg, 4, 4, K_4);

	page p, s;
	ASSERT(seg.assign_page(&p) == sdb::SUCCESS);
	ASSERT(seg.get_block_count() == 1);
	ASSERT(p.length == K_4 * kilo_byte - sdb::tree::index_block_header_size);
	std::cout << "node_size: " << p.header->node_size << std::endl;

	ASSERT(seg.assign_page(&s) == sdb::SUCCESS);
	ASSERT(seg.get_block_count() == 2);
	ASSERT(p.test_flag(sdb::storage::REMOVED_BLK_BIT) == false);

	ASSERT(p.is_root());

	node2 n;
	p.assign_node(&n);
	ASSERT(n.length == 4 + 4);

	int i = 0xA0B1C2D3;
	int t = 0xE0F09180;
	short l = sizeof(int);
	n.set_key_val(to_chars(i), l, to_chars(t), l);
	ASSERT(to_int(n.k.v) == i);
	ASSERT(to_int(n.v.v) == t);

	seg.flush();
	df.close();

	//test read
	df.open();
	seg.free_buffer();
	df.fetch_segment(seg);
	std::cout << "seg+block_count:" << seg.get_block_count() << std::endl;
	ASSERT(seg.get_block_count() == 2);
	df.close();

	page rp;
	rp.offset = 0;
	ASSERT(seg.read_page(rp) == sdb::SUCCESS);
	ASSERT(rp.header->blk_magic == sdb::storage::block_magic);
	ASSERT(rp.header->node_count==1);
	node2 nr;
	nr.offset = 0;
	ASSERT(rp.read_node(&nr) == sdb::SUCCESS);
	ASSERT(nr.offset == 0);
	ASSERT(to_int(nr.k.v) == i);
	ASSERT(to_int(nr.v.v) == t);
}

void runSuite() {
	cute::suite s;
	s.push_back(CUTE(test_avl));
	s.push_back(CUTE(tree_node_test));
	s.push_back(CUTE(basic_test_bptree));
	s.push_back(CUTE(basic_page_test));
	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "Sdb Tree Suite");
}

int main() {
	runSuite();
	return 0;
}

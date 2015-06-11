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

void basic_segment_test() {
	data_file df(1L, "/tmp/bptree.db");
	if (df.exist()) {
		df.remove();
	}
	df.open();

	index_segment root_seg(1L, &df, 0);
	root_seg.set_length(M_64);

	df.assgin_segment(root_seg);
	df.close();

	ASSERT(root_seg.get_seg_type() == sdb::storage::index_segment_type);
//	index_segment r_seg = std::move(root_seg);
	index_segment r_seg(root_seg);

	ASSERT(r_seg.get_length() == root_seg.get_length());
	ASSERT(r_seg.get_seg_type() == sdb::storage::index_segment_type);
	ASSERT(df.open() == sdb::SUCCESS);

	ASSERT(df.fetch_segment(r_seg) == sdb::SUCCESS);
	ASSERT(r_seg.has_buffer());
	ASSERT(r_seg == root_seg);

	ASSERT(root_seg.has_buffer() == false);
}

void page_node_test() {
	data_file df(1L, "/tmp/bptree.db");
	if (df.exist()) {
		df.remove();
	}
	df.open();

	node2::head n_header;
	int n_size = sizeof(n_header);
	ASSERT(n_size == node2_header_size);

	index_segment seg(1L, &df, 0);
	seg.set_length(M_64);
	seg.set_block_size(K_4);
	seg.assign_content_buffer();

	// before assign a index page, must know the key length and value length,
	// whereas create a bptree object
	bptree tree(&seg, 10, 4, 4, K_4);

	page p1, p2;

	ASSERT(seg.assign_page(&p1) == sdb::SUCCESS);
	int p1_off = p1.offset;
	ASSERT(seg.get_block_count() == 2);
	ASSERT(p1.idx_seg == &seg);
	ASSERT(p1.length == K_4 * kilo_byte - sdb::tree::index_block_header_size);
	ASSERT(seg.assign_page(&p2) == sdb::SUCCESS);
	ASSERT(seg.get_block_count() == 3);
	ASSERT(p1.test_flag(sdb::storage::REMOVED_BLK_BIT) == false);
	ASSERT(p1.ref_flag && p2.ref_flag);

	ASSERT(p1.is_root());

	// add node to page
	node2 n, n1;
	p1.assign_node(&n);
	p1.assign_node(&n1);
	ASSERT(n.length == 4 + 4);
	ASSERT(n1.length == 4 + 4);
	ASSERT(n1.ref_flag && n.ref_flag);
	ASSERT(p1.ref_flag && p2.ref_flag);

	int i = 0xA0B1C2D3;
	int i1 = 0xA0B1C2D0;
	int t = 0xE0F09180;
	int t1 = 0xE0F0918a;
	short l = sizeof(int);
	n.set_key_val(to_chars(i), l, to_chars(t), l);
	ASSERT(n.k.ref && n.v.ref);
	ASSERT(to_int(n.k.v) == i);
	ASSERT(to_int(n.v.v) == t);
	n1.set_key_val(to_chars(i1), l, to_chars(t1), l);
	ASSERT(p1.ref_flag && p2.ref_flag);
	ASSERT((n < n1) == false && n1 > n);

	seg.flush();
	df.close();

	//test read
	df.open();
	seg.free_buffer();
	df.fetch_segment(seg);
	ASSERT(seg.get_block_count() == 3);
	df.close();

	page rp;
	rp.offset = p1_off;
	ASSERT(seg.read_page(&rp) == sdb::SUCCESS);
	ASSERT(rp.header->blk_magic == sdb::storage::block_magic);
	ASSERT(rp.header->node_count == 2);

	node2 nr;
	node2::head h;
	h.flag = -1;
	nr.header = &h;
	nr.offset = 0;
	ASSERT(rp.read_node(&nr) == sdb::SUCCESS);
	ASSERT(rp.ref_flag && nr.ref_flag);
	ASSERT(nr.offset == 0);
	ASSERT(nr.header->flag == 0);
	ASSERT(nr.buffer == rp.buffer + node2_header_size);
	ASSERT(nr.k.v == nr.buffer);
	ASSERT(nr.v.v == nr.buffer + 4);
	ASSERT(to_int(nr.k.v) == i);
	ASSERT(to_int(nr.v.v) == t);
	ASSERT(rp.ref_flag && nr.ref_flag);

}

void basic_page_test() {
	data_file df(1L, "/tmp/bptree.db");
	if (df.exist()) {
		df.remove();
	}
	df.open();

	page::head p_header;
	data_block::head d_header;
	int p_size = sizeof(p_header);
	int d_size = sizeof(d_header);
	ASSERT(p_size == index_block_header_size && d_size == block_header_size);

	index_segment seg(1L, &df, 0);
	seg.set_length(M_64);
	seg.set_block_size(K_4);
	seg.assign_content_buffer();

	// before assign a index page, must know the key length and value length,
	// whereas create a bptree object
	bptree tree(&seg, 10, 4, 4, K_4);
	ASSERT(seg.get_block_count() == 1);
	page p1, p2;

	int t_len = K_4 * kilo_byte - sdb::tree::index_block_header_size;
	ASSERT(seg.assign_page(&p1) == sdb::SUCCESS);
	ASSERT(seg.get_block_count() == 2);
	ASSERT(p1.idx_seg == &seg);
	ASSERT(p1.length == t_len);
	ASSERT(seg.assign_page(&p2) == sdb::SUCCESS);
	ASSERT(p2.length == t_len);
	ASSERT(seg.get_block_count() == 3);
	ASSERT(p1.test_flag(sdb::storage::REMOVED_BLK_BIT) == false);
	ASSERT(p1.ref_flag && p2.ref_flag);

	ASSERT(p1.is_root() && p1.is_leaf_page());
}

void basic_bptree_test() {
	data_file df(1L, "/tmp/basic_bptree_test.db");
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
	bptree tree(&seg, 10, 4, 4, K_4);
	ASSERT(tree.get_m() == 10);

	common::char_buffer kcb(20), vcb(4);
	vcb << 0xDDDDDDDD;
	for (int i = 0; i < 10; i++) {
		kcb << i + 1;
		key k;
		val v;
		k.ref_val(kcb.data() + i * 4, 4);
		v.ref_val(vcb.data(), 4);

		ASSERT(tree.add_key(k, v) == sdb::SUCCESS);
	}

	page fp;
	fp.offset = 0;
	ASSERT(tree.get_root_seg()->read_page(&fp) == sdb::SUCCESS);
	ASSERT(fp.header->node_count == 1);

	df.update_segment(seg);
	seg.free_buffer();
	df.close();
	df.open();
	df.fetch_segment(seg);
	ASSERT(seg.get_block_count() == 3);
	df.close();

	// test for read
	page pr; // root page;
	pr.offset = 0;
	ASSERT(seg.read_page(&pr) == sdb::SUCCESS);
	// at this time, the root page only has one node
	node2 rn;
	rn.offset = 0;
	pr.read_node(&rn);
	ASSERT(to_int(rn.k.v) == 6);
	ASSERT(rn.test_flag(ND2_LP_BIT) && rn.test_flag(ND2_RP_BIT));
	ASSERT(rn.header->left_pg_off == 4096 && rn.header->right_pg_off == 8192);

	page left_page, right_page;
	left_page.offset = rn.header->left_pg_off;
	seg.read_page(&left_page);
	right_page.offset = rn.header->right_pg_off;
	seg.read_page(&right_page);
	ASSERT(
			left_page.header->parent_blk_off == 0
					&& left_page.header->parent_nd2_off == 0
					&& left_page.header->node_count == 5);
	ASSERT(
			right_page.header->parent_blk_off == 0
					&& right_page.header->parent_nd2_off == 0
					&& right_page.header->node_count == 5);

	// test for page split_2_3
	bptree existed_tree(&seg, 10, 4, 4, K_4);

	for (int i = 10; i < 20; i++) {
		kcb << i + 1;
		key k;
		val v;
		k.ref_val(kcb.data() + i * 4, 4);
		v.ref_val(vcb.data(), 4);
		ASSERT(existed_tree.add_key(k, v) == sdb::SUCCESS);
	}

	pr.print_all();

	ASSERT(seg.get_block_count() == 4);
	ASSERT(pr.header->node_count == 2);

	/*
	 * after split_2_3, the old parent node loses its right page flag, its next
	 * node indicates it has hidden shadow right page. the next node is the last
	 * node in the page. it has left page, either right page.
	 */
	node2 pn1, pn2; // parent node

	pn1.offset = pr.header->node_size * 0;
	pn2.offset = pr.header->node_size * 1;
	pr.read_node(&pn1);
	pr.read_node(&pn2);

	ASSERT(pn1.test_flag(ND2_LP_BIT) && pn1.header->left_pg_off == 4096);
	ASSERT(pn1.test_flag(ND2_RP_BIT) == false);
	ASSERT(pn2.test_flag(ND2_LP_BIT) && pn2.test_flag(ND2_RP_BIT));
	ASSERT(pn2.header->left_pg_off == 8192 && pn2.header->right_pg_off == 12288);


	// test for split_2_3
	for (int i = 20; i < 30; i++) {
		kcb << i + 1;
		key k;
		val v;
		k.ref_val(kcb.data() + i * 4, 4);
		v.ref_val(vcb.data(), 4);
		ASSERT(existed_tree.add_key(k, v) == sdb::SUCCESS);
	}

	ASSERT(seg.get_block_count() == 5);
	ASSERT(pr.header->node_count == 3);

	df.update_segment(seg);

	pr.print_all();

	page p;
	for (int i = 0; i < 4; i++) {
		p.offset = (i + 1) * 4096;
		seg.read_page(&p);
//		std::cout << "----- print page node offset: " << p.offset << std::endl;
//		p.print_all();
	}

	/* --------------------------------------------------------
	 *
	 * tptree scan test
	 *
	 *--------------------------------------------------------*/
	page sp;
	node2 sn;
	key sk1, sk2;
	kcb.rewind();
	kcb << 27 << 6;
	sk1.ref_val(kcb.data(), 4);
	sk2.ref_val(kcb.data() + 4, 4);
	ASSERT(existed_tree.scan(sk1, scan_greater, sp, sn) == sdb::SUCCESS);
	ASSERT(to_int(sn.k.v) == 28);

	ASSERT(existed_tree.scan(sk2, scan_less, sp, sn));
	ASSERT(to_int(sn.k.v) == 5);

	/*-----------------------------------------------------
	 *
	 * test for tree delete
	 *
	 *--------------------------------------------------------*/

	ASSERT(existed_tree.remove_key(sk1));
	ASSERT(existed_tree.scan(sk1, scan_equal, sp, sn) == KEY_NOT_IN_RANGE);

	key rk;
	kcb.rewind();
	for (int i=0; i<10; i++) {
		kcb << i+1 ;
		rk.ref_val(kcb.data() + i * 4, 4);
		ASSERT(existed_tree.remove_key(rk));
	}
	std::cout << "after remove first 10 node:::" << std::endl;
	pr.print_all();
	df.close();
}

void runSuite() {
	cute::suite s;
	s.push_back(CUTE(test_avl));
	s.push_back(CUTE(tree_node_test));
	s.push_back(CUTE(basic_segment_test));
	s.push_back(CUTE(basic_page_test));
	s.push_back(CUTE(page_node_test));
	s.push_back(CUTE(basic_bptree_test));

	cute::ide_listener lis;
	cute::makeRunner(lis)(s, "Sdb Tree Suite");
}

int main() {
	runSuite();
	return 0;
}

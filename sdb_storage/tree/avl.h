/*
 * avl.h
 *
 *  Created on: May 5, 2015
 *      Author: linkqian
 */

#ifndef TREE_AVL_H_
#define TREE_AVL_H_

#include <queue>
#include "node.h"

#include <iostream>

namespace sdb {
namespace tree {

enum search_type {
	equal = 1,
	less = 2,
	greater = 4,
	less_equal = equal | less,
	greater_equal = equal | greater
};

/*
 * Stepping through the items of a tree, by means of the connections between parents
 * and children, is called walking the tree, and the action is a walk of the tree.
 * Often, an operation might be performed when a pointer arrives at a particular node.
 */
enum traverse_order {
	pre_order, /* a walk in which each parent node is traversed before its children */
	in_order, /* a walk in which a node's left subtree, then the node itself,
	 and finally its right subtree are traversed */
	post_order, /* a walk in which the children are traversed before
	 their respective parents are traversed */
	level_order /* nodes are traversed level by level, where the root node is visited first,
	 followed by its direct child nodes and their siblings, until all nodes in the tree have been traversed.*/
};

enum node_handler_event {
	not_found,
	equal_event,
	greater_event,
	less_event,
	add_left_event,
	add_right_event,
	overwrite_event,
	rotate_left_event,
	rotate_right_event
};

/*
 * a simple node handler when access a node, sub-class of node_handler must
 * implement the handle(const node<K,V> *n) method
 */
template<class K, class V>
class node_handler {
public:

	virtual void handle(const node<K, V> * n) {
	}
	virtual void handle(const node<K, V> *p, const node<K, V> * n,
			node_handler_event event) {
	}

	virtual void handle(const node<K, V> *p, const K k,
			node_handler_event event) {
	}
};

/*
 * AVL tree implementation referenced https://en.wikipedia.org/wiki/AVL_tree
 */
template<class K, class V>
class avl {
private:
	node<K, V> *root = nullptr;

	/*
	 * the n is rotate node, its parent balance factor is -2 or 2
	 */
	void balance(node<K, V> *n) {
		if (n->has_parent()) {
			node<K, V> *p = n->parent;
			int bf = p->balance_factor();
			if (bf >= 2) {
				n->rotate_right();
				if (n->is_root()) {
					root = n;
				}
			} else if (bf <= -2) {
				n->rotate_left();
				if (n->is_root()) {
					root = n;
				}
			}

			// recursively balance if  the newest node has parent
			if (n->has_parent()) {
				balance(n->parent);
			}
		}

//		if (n->is_root()) {
//			root = n;
//		} else if (n->has_parent() && n->parent->is_root()) {
//			root = n->parent;
//		}

	}

	node<K, V> * find_equal(node<K, V> *n, const K k) {
		if (n->k == k) {
			return n;
		} else if (k < n->k) {
			if (n->has_left()) {
				return find_equal(n->left, k);
			} else {
				return nullptr;
			}
		} else {
			if (n->has_right()) {
				return find_equal(n->right, k);
			} else {
				return nullptr;
			}
		}
	}

	node<K, V> * find_less(node<K, V> *n, const K k) {
		if (n->k < k) {
			if (n->has_right()) {
				if (n->right->k >= k) {
					return n;
				} else {
					return find_less(n->right, k);
				}
			} else {
				return n;
			}
		} else {
			return nullptr;
		}
	}

	node<K, V> * find_greater(node<K, V> *n, const K k) {
		if (n->k > k) {
			if (n->has_left()) {
				if (n->left->k <= k) {
					return n;
				} else {
					return find_greater(n->left, k);
				}
			} else {
				return n;
			}
		} else {
			return nullptr;
		}
	}

	node<K, V> * find_less_equal(node<K, V> *n, const K k) {
		if (n->k <= k) {
			if (n->has_right()) {
				if (n->right->k > k) {
					return n;
				} else {
					return find_less(n->right, k);
				}
			} else {
				return n;
			}
		} else {
			return nullptr;
		}
	}

	node<K, V> * find_greater_equal(node<K, V> *n, const K k) {
		if (n->k >= k) {
			if (n->has_left()) {
				if (n->left->k < k) {
					return n;
				} else {
					return find_greater(n->left, k);
				}
			} else {
				return n;
			}
		} else {
			return nullptr;
		}
	}

	void trav_pre_ord(node<K, V> *n, node_handler<K, V> *h) {
		h->handle(n);
		if (n->has_left()) {
			trav_pre_ord(n->left, h);
		}
		if (n->has_right()) {
			trav_pre_ord(n->right, h);
		}
	}

	void trav_in_ord(node<K, V> *n, node_handler<K, V> *h) {
		if (n->has_left()) {
			trav_in_ord(n->left, h);
		}
		h->handle(n);
		if (n->has_right()) {
			trav_in_ord(n->right, h);
		}
	}

	void trav_post_ord(node<K, V> *n, node_handler<K, V> *h) {
		if (n->has_left()) {
			trav_post_ord(n->left, h);
		}
		if (n->has_right()) {
			trav_post_ord(n->right, h);
		}
		h->handle(n);
	}

	void trav_level_ord(node<K, V> *n, node_handler<K, V> *h) {
		std::queue<node<K, V> *> queue;
		queue.push(n);
		while (!queue.empty()) {
			node<K, V> *p = queue.front();
			queue.pop();
			if (p->has_left()) {
				queue.push(p->left);
			}

			if (p->has_right()) {
				queue.push(p->right);
			}
			h->handle(p);
		}
	}

public:
	/*
	 * recursive calculate the avl tree height, it starts at root node
	 */
	int height() {
		return root->height();
	}

	int balance_factor() {
		return root->balance_factor();
	}

	/*
	 * find a node which operate with search type, the default search type is equal.
	 * list operator means that:
	 *
	 * 1) equal, return the only one node which k equals the searching k
	 *
	 * 2) less,  return the only one node which k is less than the searching k, but
	 * 	the node's right child is greater or equal than the searching k
	 *
	 * 3) greater, return the only one node which k is greater than the searching k,
	 *  but the node's left child is less than the searching k
	 *
	 * 4) less_equal, return the only one node which k is less or equal than the searching k,
	 *  but the node's right child is greater than the searching k.
	 *
	 * 5) greater_equal, return the only one node which k is greater or equal than the
	 *  searching k, but the node's left child is less than the searching k.
	 *
	 */
	node<K, V> *find(K k, search_type st) {
		switch (st) {
		case less:
			return find_less(root, k);
		case greater:
			return find_greater(root, k);
		case less_equal:
			return find_less_equal(root, k);
		case greater_equal:
			return find_greater_equal(root, k);
		default:
			return find_equal(root, k);
		}
	}

	bool exists(const K k) {
		return root->exists(k);
	}

	bool exists(const node<K, V> *n) {
		return n->exists(n->k);
	}

	void traverse(node<K, V> *n, node_handler<K, V> *h, traverse_order ord =
			in_order) {
		switch (ord) {
		default:
			trav_in_ord(n, h);
			break;
		case pre_order:
			trav_pre_ord(n, h);
			break;
		case post_order:
			trav_post_ord(n, h);
			break;
		case level_order:
			trav_level_ord(n, h);
			break;
		}
	}

	/*
	 * traverse the avl start at the root
	 */
	void traverse(node_handler<K, V> *h, traverse_order ord = in_order) {
		traverse(root, h, ord);
	}

	/*
	 * insert a node denoted by n to parent node
	 * 1) if the node K does not exist in the tree, insert the node ,return true
	 *
	 * 2) if the node K already exists, and the overwrite flag is true,
	 *  overwrite the existed node, return true, else return false
	 */
	bool insert_node(node<K, V> *parent, node<K, V> * n,
			bool overwrite = false) {
		if (parent->k == n->k) {
			if (overwrite) {
				parent = n;
				return true;
			} else {
				return false;
			}
		} else if (parent->k > n->k) {
			if (parent->has_left()) {
				return insert_node(parent->left, n, overwrite);
			} else {
				parent->add_left(n);
				// if parent's parent balance factor is -2, or 2, balance it
				if (!parent->is_root()) {
					balance(parent->parent);
				}
				return true;
			}
		} else {
			if (parent->has_right()) {
				return insert_node(parent->right, n, overwrite);
			} else {
				parent->add_right(n);
				if (!parent->is_root()) {
					balance(parent->parent);
				}
				return true;
			}
		}
	}

	/*
	 * short method of node insertion starting at the root node
	 */
	bool insert_node(node<K, V> * n, bool overwrite = false) {
		if (root == nullptr) {
			root = n;
			return true;
		} else {
			return insert_node(root, n, overwrite);
		}
	}

	/*
	 * find a node's k equal k given, remove the node from avl, and return it;
	 *
	 * when remove a node from avl, a child of the node should be promoted.
	 * there are two types of child node promotion:
	 *
	 * 1) left promotion, in case of the removing node key is less than its parent key
	 *  or the removing node is root. if the removing node has left and right child
	 *  node, add the right node to last right leaf of promotion node as new right leaf
	 *
	 * 2) right promotion, in case of the removing node key is greater than is parent key
	 *  if the removing node has left and right child node, add the left child node to the
	 *  left leaf of the promotion node as new left leaf.
	 *
	 */
	node<K, V> * remove(K k) {
		node<K, V> *d = find_equal(root, k);
		if (d != nullptr) {
			if (d->is_root()) {
				if (d->is_leaf()) { // avl has only one node
					root = nullptr;
					return d;
				} else {
					// promotion root's left child, the deleting node's right
					// child as the new right leaf of root's, needn't balance
					root = d->left;
					root->parent = nullptr;
					root->right_leaf()->add_right(d->right);

					// ? may need to remove children on the old root
				}
			} else if (d->is_leaf()) {
				if (d->is_left()) {
					d->parent->remove_left_leaf();
				} else {
					d->parent->remove_right_leaf();
				}
				balance(d->parent);
			} else { // middle node
				node<K, V> *prom;
				if (d->has_left()) {  // the deleting node has left child node
					prom = d->left;
					d->k < d->parent->k ?
							d->parent->add_left(prom) :
							d->parent->add_right(prom);
					if (d->has_right()) {
						if (prom->has_right()) {
							prom->right_leaf()->add_right(d->right);
						} else {
							prom->add_right(d->right);
						}
					}
				} else { //only has right child node
					prom = d->right;
					d->k < d->parent->k ?
							d->parent->add_left(prom) :
							d->parent->add_right(prom);
				}

				balance(prom->parent);
			}
			return d;
		} else {
			return nullptr;
		}
	}

	/*
	 * delete all node and free memory of them
	 */
	void purge() {

	}

	/*
	 * default constructor that create an empty avl
	 */
	avl() {
	}
	/*
	 * constructor create an avl include a root node
	 */
	avl(node<K, V> *_r) {
		root = _r;
	}

	avl(const avl & _other) {
		root = _other.root;
	}

	avl & operator=(const avl & _other) {
		avl t;
		t.root = _other.root;
		return t;
	}

	virtual ~avl() {
	}
};

} /* namespace tree */

} /* namespace sdb */

#endif /* TREE_AVL_H_ */

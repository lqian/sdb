/*
 * node.hpp
 *
 *  Created on: May 7, 2015
 *      Author: linkqian
 */

#ifndef TREE_NODE_H_
#define TREE_NODE_H_

namespace sdb {
namespace tree {

/*
 * template class of K, the K must implements operator >, = and < method if
 * it is not primitive type.
 */
template<class K, class V>
struct node {
	K k;
	V v;
	node * parent = nullptr; /* point parent */
	node * left = nullptr; /* point to left child */
	node * right = nullptr; /* point right child */

	bool is_root() {
		return parent == nullptr;
	}

	bool has_left() {
		return left != nullptr;
	}

	bool has_right() {
		return right != nullptr;
	}

	bool has_parent() {
		return parent != nullptr;
	}

	bool is_leaf() {
		return left == nullptr && right == nullptr;
	}

	bool is_right() {
		return parent->right == this;
	}

	bool is_left() {
		return parent->left == this;
	}

	bool is_middle() {
		return parent != nullptr && (has_left() || has_right());
	}

	void remove_left_leaf() {
		if (left->is_leaf()) {
			left->parent = nullptr;
			left = nullptr;
		}
	}

	void remove_right_leaf() {
		if (right->is_leaf()) {
			right->parent = nullptr;
			right = nullptr;
		}
	}

	bool exists(K t) {
		if (k == t) {
			return true;
		} else if (k < t) {
			return has_right() && right->exists(t);
		} else if (k > t) {
			return has_left() && left->exists(t);
		} else {
			return false;
		}
	}

	bool is_null() {
		return parent == nullptr && left == nullptr && right == nullptr
				&& k.is_null() && v.is_null();
	}

	int height() {
		int hr = 0, hl = 0;
		if (is_leaf()) {
			return 1;
		}
		if (has_right()) {
			hr += right->height();
		}
		if (has_left()) {
			hl += left->height();
		}

		return hr > hl ? hr + 1 : hl + 1;
	}

	/*
	 * the term stage is the number of node include its children and itself,
	 * the a node stage is 1 means that it is a leaf.
	 */
	int stage() {
		int s = 0;
		if (is_leaf()) {
			s++;
		} else {
			s++;
			if (has_right()) {
				s += right->stage();
			}
			if (has_left()) {
				s += left->stage();
			}
		}
		return s;
	}

	int balance_factor() {
		int hl = has_left() ? left->height() : 0;
		int hr = has_right() ? right->height() : 0;
		return hl - hr;
	}

	/*
	 * this node is promoted as parent, its parent is moved to left child of the node.
	 * this node is promotion node (PN),  meanwhile its parent node
	 * is moving to the left child (ML), the left child node of PN is pivotal
	 * node which should be moved to the ML node's right child
	 *
	 *
	 * THE METHOD MUST NOT APPLY ON ROOT NODE
	 *
	 */
	void rotate_left() {
		node * gp = parent->parent;
		if (has_left()) {
			parent->add_right(left);
		} else {
			parent->right = nullptr;
		}
		add_left(parent);

		if (gp != nullptr) {
			gp->add_left(this);
		} else {
			parent = gp;
		}
	}

	/*
	 * this node is promoted as parent, its parent is moved to right child of the node.
	 * this node is promotion node (PN),  meanwhile its parent node
	 * is moving to the right child (MR), the right child node of PN is pivotal
	 * node which should be moved to the MR node's left child
	 *
	 * THE METHOD MUST NOT APPLY ON ROOT NODE
	 *
	 */
	void rotate_right() {
		node * gp = parent->parent;
		if (has_right()) {
			parent->add_left(right);
		} else {
			parent->left = nullptr;
		}
		add_right(parent);
		if (gp != nullptr) {
			gp->add_right(this);
		} else {
			parent = gp;
		}
	}

	void add_right(node *n) {
		n->parent = this;
		right = n;
	}

	void add_left(node *n) {
		n->parent = this;
		left = n;
	}

	node * left_leaf() {
		node * ll = left;
		while (ll->has_left()) {
			ll = ll->left;
		}
		return ll;
	}

	node * right_leaf() {
		node *rl = right;
		while (rl->has_right()) {
			rl = rl->right;
		}
		return rl;
	}
};
} /* namespace tree */

} /* namespace sdb */
#endif /* TREE_NODE_H_ */

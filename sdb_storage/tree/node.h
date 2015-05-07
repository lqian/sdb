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

	void remove_as_leaf(node *n) {
		if (n == left) {
			left = nullptr;
			n->parent = nullptr;
		} else if (n == right) {
			right = nullptr;
			n->parent = nullptr;
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
	 * rotate this node to be the left child node of its right node.
	 * this node is to be rotate left node,
	 * the right child node is promotion node, its left child node is pivotal node
	 *
	 */
	void rotate_left() {
		if (has_right()) {
			right->parent = parent;
			if (!is_root()) {
				parent->right = this->right;
			}

			if (right->has_left()) {
				add_right(right->left);
			}
			right->add_left(this);
			right = nullptr;
		}
	}

	/*
	 * rotate this node to be the right node of its left node.
	 * this node is to be rotate right.
	 * the left child node is promotion node, its right child node is pivotal node
	 */
	void rotate_right() {
		if (has_left()) {
			left->parent = parent;
			if (!is_root()) {
				parent->left = this->left;
			}

			if (left->has_right()) {
				add_left(left->right);
			}
			left->add_right(this);
			left = nullptr;
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

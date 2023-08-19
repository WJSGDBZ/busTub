#include "primer/trie.h"
#include <ctime>
#include <iostream>
#include <iterator>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> root = root_;
  if (root == nullptr) {
    return nullptr;
  }

  for (auto index : key) {
    auto iter = root->children_.find(index);
    if (iter == root->children_.end()) {
      return nullptr;
    }
    root = iter->second;
  }

  if (key.empty()) {
    auto iter = root->children_.find('\0');
    root = iter->second;
  }

  if (!root->is_value_node_) {
    return nullptr;
  }

  auto value = dynamic_cast<const TrieNodeWithValue<T> *>(root.get());
  if (value) {
    return value->value_.get();
  }

  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> root;
  if (root_ == nullptr) {
    root = std::make_shared<TrieNode>();
  } else {
    root = root_->Clone();
  }
  std::shared_ptr<TrieNode> parent = root;
  // walk through key without last char and create new node
  auto it = key.begin();
  while (it != key.end() && std::next(it) != key.end()) {
    auto iter = parent->children_.find(*it);
    std::shared_ptr<TrieNode> node;
    if (iter != parent->children_.end()) {
      node = iter->second->Clone();
    } else {
      node = std::make_shared<TrieNode>();
    }

    parent->children_[*it] = node;
    parent = node;
    it++;
  }

  // process the last char
  std::shared_ptr<T> pvalue = std::make_shared<T>(std::move(value));
  auto iter = parent->children_.find(*it);
  if (key.empty() || iter == parent->children_.end()) {
    parent->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(pvalue);
  } else {
    auto children = iter->second;
    parent->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(children->children_, pvalue);
  }

  std::shared_ptr<Trie> new_trie = std::make_shared<Trie>();
  new_trie->root_ = root;
  return *new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<TrieNode> root;
  if (root_ == nullptr) {
    root = std::make_shared<TrieNode>();
  } else {
    root = root_->Clone();
  }
  std::shared_ptr<TrieNode> parent = root;
  // walk through key without last char and create new node
  auto it = key.begin();
  while (it != key.end() && std::next(it) != key.end()) {
    auto iter = parent->children_.find(*it);
    if (iter == parent->children_.end()) {
      return *this;
    }

    std::shared_ptr<TrieNode> node = iter->second->Clone();
    parent->children_[*it] = node;
    parent = node;
    it++;
  }

  // process the last char
  char last = *it;
  auto iter = parent->children_.find(last);

  if (iter != parent->children_.end()) {
    std::shared_ptr<const TrieNode> children = iter->second;
    if (children->children_.empty()) {
      parent->children_.erase(last);
    } else {
      std::shared_ptr<TrieNode> node = std::make_shared<TrieNode>(children->children_);
      parent->children_[last] = node;
    }
  }

  std::shared_ptr<Trie> new_trie = std::make_shared<Trie>();
  new_trie->root_ = root;
  return *new_trie;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

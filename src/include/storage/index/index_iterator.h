//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT

  explicit IndexIterator(BufferPoolManager *bpm, page_id_t current_page_id, ReadPageGuard &&current_page,
                         int current_index = 0);
  explicit IndexIterator(page_id_t current_page_id);

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return itr.current_page_id_ == current_page_id_ && itr.current_index_ == current_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(itr.current_page_id_ == current_page_id_ && itr.current_index_ == current_index_);
  }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  page_id_t current_page_id_;
  ReadPageGuard current_page_;
  int current_index_;
};

}  // namespace bustub

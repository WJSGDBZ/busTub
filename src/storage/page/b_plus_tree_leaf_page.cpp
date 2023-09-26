//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ENSURE(index < GetSize(), "LeafPage KeyAt index out of bound");

  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ENSURE(index < GetSize(), "LeafPage ValueAt index out of bound");

  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyValueAt(int index) const -> const MappingType & {
  BUSTUB_ENSURE(index < GetSize(), "LeafPage KeyValueAt index out of bound");

  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  BUSTUB_ENSURE(index < GetSize(), "LeafPage SetKeyValueAt index out of bound");
  array_[index] = std::make_pair(key, value);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() -> MappingType {
  BUSTUB_ENSURE(GetSize() > 0, "LeafPage PopBack empty page");

  int len = GetSize();
  IncreaseSize(-1);

  return array_[len - 1];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() -> MappingType {
  BUSTUB_ENSURE(GetSize() > 0, "LeafPage PopFront empty page");

  MappingType data = array_[0];
  int len = GetSize();
  for (int i = 0; i < len; i++) {
    std::swap(array_[i], array_[i + 1]);
  }

  IncreaseSize(-1);

  return data;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage *new_page, KeyType *key, KeyComparator &comparator_) -> bool {
  BUSTUB_ENSURE(GetSize() == GetMaxSize(), "Can only split on a full page");

  *key = array_[GetMinSize()].first;
  int len = GetMaxSize() - GetMinSize();
  for (int i = 0; i < len; i++) {
    MappingType data = PopBack();
    new_page->Insert(data.first, data.second, comparator_);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *new_page, KeyComparator &comparator_) -> bool {
  BUSTUB_ENSURE(GetSize() + new_page->GetSize() < GetMaxSize(), "Merge page spill MaxSize");

  int len = new_page->GetSize();
  for (int i = 0; i < len; i++) {
    MappingType data = new_page->PopBack();
    Insert(data.first, data.second, comparator_);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator_)
    -> bool {
  BUSTUB_ENSURE(GetSize() < GetMaxSize(), "LeafPage Insert out of bound");

  for (int i = 0; i < GetSize(); i++) {
    int compare = comparator_(key, array_[i].first);
    if (compare == 0) {
      return false;
    }
    if (compare < 0) {
      break;
    }
  }

  int len = GetSize();
  array_[len] = std::make_pair(key, value);
  for (int i = len; i >= 1; i--) {
    int compare = comparator_(array_[i].first, array_[i - 1].first);
    if (compare > 0) {
      break;
    }

    std::swap(array_[i], array_[i - 1]);
    // KeyType tmp = array_[i].first;

    // array_[i] = std::make_pair(array_[i+1].first, array_[i].second);
    // array_[i+1] = std::make_pair(tmp, array_[i+1].second);
  }
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator &comparator_) -> bool {
  int len = GetSize();
  int index = -1;
  for (int i = 0; i < len; i++) {
    int compare = comparator_(array_[i].first, key);
    if (compare > 0) {
      break;
    }
    if (compare == 0) {
      index = i;
      break;
    }
  }

  if (index != -1) {
    for (int i = index; i < len; i++) {
      std::swap(array_[i], array_[i + 1]);
    }

    IncreaseSize(-1);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsStealable() -> bool { return GetSize() > GetMinSize(); }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

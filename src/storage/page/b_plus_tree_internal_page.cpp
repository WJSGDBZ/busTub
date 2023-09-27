//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ENSURE(index < GetSize(), "InternalPage KeyAt index out of bound");

  return array_[index + 1].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ENSURE(index < GetSize(), "InternalPage SetKeyAt index out of bound");

  auto sec = array_[index + 1].second;
  array_[index + 1] = std::make_pair(key, sec);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  BUSTUB_ENSURE(index < GetSize(), "InternalPage SetKeyValueAt index out of bound");
  array_[index + 1] = std::make_pair(key, value);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ENSURE(index <= GetSize(), "InternalPage ValueAt index out of bound");

  return array_[index].second;
}

/*
 * Helper method to insert new key (not duplicate guaratee)
 * return index of new key in array (array start at index 1)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator_)
    -> bool {
  BUSTUB_ENSURE(GetSize() < GetMaxSize(), "InternalPage Insert index out of bound");

  int len = GetSize() + 1;
  array_[len] = std::make_pair(key, value);
  for (int i = len - 1; i >= 1; i--) {
    int compare = comparator_(array_[i + 1].first, array_[i].first);
    if (compare > 0) {
      break;
    }

    std::swap(array_[i + 1], array_[i]);
    // KeyType tmp = array_[i].first;

    // array_[i] = std::make_pair(array_[i+1].first, array_[i].second);
    // array_[i+1] = std::make_pair(tmp, array_[i+1].second);
  }
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ENSURE(index <= GetSize(), "InternalPage SetValueAt index out of bound");

  auto key = array_[index].first;
  array_[index] = std::make_pair(key, value);
}

/*
 * Helper method to split full page into two half-full page
 * return new_page first key(key)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *new_page, KeyType *key, KeyComparator &comparator_)
    -> bool {
  BUSTUB_ENSURE(GetSize() == GetMaxSize(), "Can only split on a full page");

  *key = array_[GetMinSize() + 1].first;
  int len = GetMaxSize() - GetMinSize();
  for (int i = 0; i < len - 1; i++) {
    MappingType data = PopBack();
    new_page->Insert(data.first, data.second, comparator_);
  }
  MappingType data = PopBack();
  new_page->SetValueAt(0, data.second);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopBack() -> MappingType {
  BUSTUB_ENSURE(GetSize() > 0, "LeafPage Pop empty page");

  int len = GetSize();
  IncreaseSize(-1);

  return array_[len];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFront() -> MappingType {
  BUSTUB_ENSURE(GetSize() > 0, "LeafPage Pop empty page");

  MappingType data = array_[1];
  int len = GetSize();
  for (int i = 1; i < len; i++) {
    std::swap(array_[i], array_[i + 1]);
  }

  IncreaseSize(-1);

  return data;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) -> bool {
  BUSTUB_ENSURE(index < GetSize(), "InternalPage Remove index out of bound");

  for (int i = index + 1; i < GetSize(); i++) {
    std::swap(array_[i], array_[i + 1]);
  }
  IncreaseSize(-1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *new_page, KeyComparator &comparator_) -> bool {
  BUSTUB_ENSURE(GetSize() + new_page->GetSize() < GetMaxSize(), "Merge page spill MaxSize");

  int len = new_page->GetSize();
  for (int i = 0; i < len; i++) {
    MappingType data = new_page->PopBack();
    Insert(data.first, data.second, comparator_);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveOn() -> bool {
  BUSTUB_ENSURE(GetSize() != 0, "can't move on empty page");

  SetValueAt(0, array_[1].second);
  IncreaseSize(-1);
  for (int i = 0; i < GetSize(); i++) {
    std::swap(array_[i + 1], array_[i + 2]);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsStealable() -> bool { return GetSize() > 1; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsInsertionSafty() -> bool { return GetSize() + 1 < GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsDeletionSafty() -> bool { return GetSize() > 1; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

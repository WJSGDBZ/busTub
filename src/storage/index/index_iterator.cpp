/**
 * index_iterator.cpp
 */
#include <cassert>
#include <memory>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t current_page_id, ReadPageGuard &&current_page,
                                  int current_index)
    : bpm_(bpm),
      current_page_id_(current_page_id),
      current_page_(std::move(current_page)),
      current_index_(current_index) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  const auto *page = current_page_.As<BPlusTreePage>();
  BUSTUB_ASSERT(page->IsLeafPage(), "iterator only iterate LeafPage");

  const auto *leaf_page = current_page_.As<LeafPage>();

  return current_index_ + 1 == page->GetSize() && leaf_page->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  const auto *page = current_page_.As<BPlusTreePage>();
  BUSTUB_ASSERT(page->IsLeafPage(), "iterator only iterate LeafPage");
  BUSTUB_ASSERT(current_page_id_ != INVALID_PAGE_ID, "Iterator already reach end");

  const auto *leaf_page = current_page_.As<LeafPage>();
  return leaf_page->KeyValueAt(current_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  const auto *page = current_page_.As<BPlusTreePage>();
  BUSTUB_ASSERT(page->IsLeafPage(), "iterator only iterate LeafPage");
  BUSTUB_ASSERT(current_page_id_ != INVALID_PAGE_ID, "Iterator already reach end");

  if (IsEnd()) {
    current_page_id_ = INVALID_PAGE_ID;
    current_index_ = 0;
  } else {
    const auto *leaf_page = current_page_.As<LeafPage>();
    if (current_index_ + 1 == page->GetSize()) {
      page_id_t next_page_id = leaf_page->GetNextPageId();

      current_page_id_ = next_page_id;
      current_page_ = bpm_->FetchPageRead(next_page_id);
      current_index_ = 0;
    } else {
      current_index_++;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

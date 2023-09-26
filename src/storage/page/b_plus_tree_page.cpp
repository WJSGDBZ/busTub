//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"
#include <algorithm>
#include "common/macros.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) {
  BUSTUB_ASSERT(size_ + amount <= max_size_, "page size should less than max_size_");
  BUSTUB_ASSERT(size_ + amount >= 0, "page size should greater than 0");
  size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int { return max_size_ / 2; }

/*
 * Helper method to check if tree is full
 */
auto BPlusTreePage::NeedsSpliting() const -> bool { return GetSize() == GetMaxSize(); }

/*
 * Helper method to check if tree need merge
 */
auto BPlusTreePage::NeedsMerging() const -> bool {
  if (!IsLeafPage()) {
    return GetSize() == 0;
  }
  return GetSize() < GetMinSize();
}

}  // namespace bustub

#include <optional>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

const bool DEBUG = false;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  if (DEBUG) {
    std::cout << "leaf_max_size = " << leaf_max_size << ", internal_max_size = " << internal_max_size << std::endl;
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return false; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  const auto *root_page = head_guard.As<BPlusTreeHeaderPage>();

  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  if (DEBUG) {
    std::cout << "GetValue " << key << std::endl;
  }
  Context ctx;
  // ctx.root_page_id_ = root_page_id;
  ReadPageGuard guard = bpm_->FetchPageRead(root_page->root_page_id_);
  // ctx.read_set_.push_back(guard);

  const auto *head = guard.As<BPlusTreePage>();
  page_id_t next_page_id = INVALID_PAGE_ID;
  while (!head->IsLeafPage()) {
    auto *inner_page = guard.As<InternalPage>();
    FindNextPage(key, inner_page, 0, head->GetSize() - 1, &next_page_id);
    guard = bpm_->FetchPageRead(next_page_id);
    // ctx.read_set_.push_back(guard);

    head = guard.As<BPlusTreePage>();
  }

  auto *leaf_page = guard.As<LeafPage>();

  return FindValueType(key, leaf_page, 0, head->GetSize() - 1, result);
}

// @Return index of value
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindNextPage(const KeyType &key, const InternalPage *inner_page, int left, int right,
                                  page_id_t *next) -> int {
  int mid = 0;

  while (left < right) {
    mid = left + (right - left) / 2;
    int compare = comparator_(key, inner_page->KeyAt(mid));
    if (compare > 0) {
      left = mid + 1;
    } else if (compare < 0) {
      right = mid - 1;
    } else {
      break;
    }
  }

  mid = left + (right - left) / 2;
  int compare = comparator_(key, inner_page->KeyAt(mid));
  if (compare < 0) {
    *next = inner_page->ValueAt(mid);
    return mid;
  }

  *next = inner_page->ValueAt(mid + 1);
  return mid + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindKeyIndex(const KeyType &key, const LeafPage *leaf_page, int left, int right, int *index)
    -> bool {
  int mid = 0;
  while (left <= right) {
    mid = left + (right - left) / 2;
    int compare = comparator_(key, leaf_page->KeyAt(mid));
    if (compare > 0) {
      left = mid + 1;
    } else if (compare < 0) {
      right = mid - 1;
    } else {
      *index = mid;
      return true;
    }
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindValueType(const KeyType &key, const LeafPage *leaf_page, int left, int right,
                                   std::vector<ValueType> *result) -> bool {
  int mid = 0;
  // int left_bound = left;
  // int right_bound = right;
  while (left <= right) {
    mid = left + (right - left) / 2;
    int compare = comparator_(key, leaf_page->KeyAt(mid));
    if (compare > 0) {
      left = mid + 1;
    } else if (compare < 0) {
      right = mid - 1;
    } else {
      result->push_back(leaf_page->ValueAt(mid));

      // continue to find left same key
      // int cur = mid-1;
      // while(cur >= left_bound){
      //   compare = comparator_(key, leaf_page->KeyAt(cur));
      //   if(compare != 0){
      //     break;
      //   }
      //   result->push_back(leaf_page->ValueAt(cur));
      //   cur--;
      // }

      // continue to find right same key
      // cur = mid+1;
      // while(cur <= right_bound){
      //   compare = comparator_(key, leaf_page->KeyAt(cur));
      //   if(compare != 0){
      //     break;
      //   }
      //   result->push_back(leaf_page->ValueAt(cur));
      //   cur++;
      // }

      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  WritePageGuard head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *head_page = head_guard.AsMut<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {  // empty tree
    // start new tree, update root page id
    page_id_t root_page_id = INVALID_PAGE_ID;
    BasicPageGuard root_guard = bpm_->NewPageGuarded(&root_page_id);
    if (!root_guard.IsValid()) {
      return false;
    }
    auto *root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    head_page->root_page_id_ = root_page_id;

    root_guard.Drop();
    head_guard.Drop();
    return Insert(key, value, txn);
  }

  Context ctx;
  ctx.root_page_id_ = head_page->root_page_id_;
  ctx.header_page_ = std::move(head_guard);
  if (DEBUG) {
    std::cout << "Insert " << key << std::endl;
  }
  return Insert(head_page->root_page_id_, key, value, txn, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(page_id_t root_page_id, const KeyType &key, const ValueType &value, Transaction *txn,
                            Context &context) -> bool {
  WritePageGuard guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = guard.AsMut<BPlusTreePage>();
  bool valid = false;

  if (root_page->IsLeafPage()) {
    auto *leaf_page = guard.AsMut<LeafPage>();
    valid = leaf_page->Insert(key, value, comparator_);
  } else {
    auto *cur_page = guard.AsMut<InternalPage>();

    // feel free to release parent latch
    if (cur_page->IsInsertionSafty()) {
      context.header_page_ = std::nullopt;
      while (!context.write_set_.empty()) {
        context.write_set_.front().Drop();
        context.write_set_.pop_front();
      }
    }

    page_id_t next_page_id = INVALID_PAGE_ID;
    FindNextPage(key, cur_page, 0, root_page->GetSize() - 1, &next_page_id);
    context.write_set_.push_back(std::move(guard));

    valid = Insert(next_page_id, key, value, txn, context);
    if (!context.write_set_.empty()) {
      guard = std::move(context.write_set_.back());
      context.write_set_.pop_back();
    }
  }

  // check if need to split
  if (root_page != nullptr && root_page->NeedsSpliting()) {
    bool done = TrySplitFullPage(root_page_id, &guard, context);
    BUSTUB_ASSERT(done, "error in BPlusTree Split, please allocate more space on bufferpool");
  }

  return valid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TrySplitFullPage(page_id_t root_page_id, WritePageGuard *guard, Context &context) -> bool {
  if (!guard->IsValid()) {
    return true;
  }

  bool valid = false;
  if (context.IsRootPage(root_page_id)) {  // root page need to split
    BUSTUB_ASSERT(context.header_page_ != std::nullopt, "TrySplitFullPage context.header_page_ should not be nullopt");
    page_id_t new_root_id = INVALID_PAGE_ID;
    BasicPageGuard new_root_guard = bpm_->NewPageGuarded(&new_root_id);
    if (new_root_guard.IsValid()) {
      page_id_t right_page_id = INVALID_PAGE_ID;
      KeyType first_key{};
      if (SplitRightSidePage(guard, &right_page_id, &first_key)) {
        auto *new_page = new_root_guard.AsMut<InternalPage>();
        new_page->Init(internal_max_size_);

        new_page->Insert(first_key, right_page_id, comparator_);
        new_page->SetValueAt(0, root_page_id);

        context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
        valid = true;
      }
    }
  } else {
    BUSTUB_ASSERT(!context.write_set_.empty(), "TrySplitFullPage parent not in context.write_set_");
    page_id_t right_page_id = INVALID_PAGE_ID;
    KeyType first_key{};

    if (SplitRightSidePage(guard, &right_page_id, &first_key)) {
      WritePageGuard parent_guard = std::move(context.write_set_.back());
      context.write_set_.pop_back();
      auto *parent_page = parent_guard.AsMut<InternalPage>();
      parent_page->Insert(first_key, right_page_id, comparator_);
      context.write_set_.push_back(std::move(parent_guard));
      valid = true;
    }
  }

  return valid;
}

/*
 * Split Full page into two half-page
 * return pageId of bigger value page (new_page_id) and first key in that page(key)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitRightSidePage(WritePageGuard *full_page_guard, page_id_t *new_page_id, KeyType *key) -> bool {
  BasicPageGuard new_guard = bpm_->NewPageGuarded(new_page_id);
  if (!new_guard.IsValid()) {
    return false;
  }

  bool vaild = false;
  auto *full_page = full_page_guard->AsMut<BPlusTreePage>();
  if (full_page->IsLeafPage()) {
    auto *leaf_page = full_page_guard->AsMut<LeafPage>();

    auto *other_page = new_guard.AsMut<LeafPage>();
    other_page->Init(leaf_max_size_);
    vaild = leaf_page->Split(other_page, key, comparator_);
    if (vaild) {
      other_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(*new_page_id);
    }
  } else {
    auto *inner_page = full_page_guard->AsMut<InternalPage>();

    auto *other_page = new_guard.AsMut<InternalPage>();
    other_page->Init(internal_max_size_);
    vaild = inner_page->Split(other_page, key, comparator_);
  }

  return vaild;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  WritePageGuard head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *head_page = head_guard.AsMut<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {  // empty tree
    return;
  }

  Context ctx;
  ctx.root_page_id_ = head_page->root_page_id_;
  ctx.header_page_ = std::move(head_guard);
  if (DEBUG) {
    std::cout << "Remove " << key << std::endl;
  }
  Remove(head_page->root_page_id_, key, txn, ctx, -1);

  ctx.header_page_ = std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(page_id_t root_page_id, const KeyType &key, Transaction *txn, Context &context,
                            int parent_index) {
  WritePageGuard guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = guard.AsMut<BPlusTreePage>();

  if (root_page->IsLeafPage()) {
    auto *leaf_page = guard.AsMut<LeafPage>();
    leaf_page->Remove(key, comparator_);
  } else {
    auto *cur_page = guard.AsMut<InternalPage>();

    // feel free to release parent latch
    if (cur_page->IsDeletionSafty()) {
      context.header_page_ = std::nullopt;
      while (!context.write_set_.empty()) {
        context.write_set_.front().Drop();
        context.write_set_.pop_front();
      }
    }

    page_id_t next_page_id = INVALID_PAGE_ID;
    int index = FindNextPage(key, cur_page, 0, root_page->GetSize() - 1, &next_page_id);
    context.write_set_.push_back(std::move(guard));
    Remove(next_page_id, key, txn, context, index);

    if (!context.write_set_.empty()) {
      guard = std::move(context.write_set_.back());
      context.write_set_.pop_back();
    }
  }

  // check if need to merge
  if (root_page != nullptr && root_page->NeedsMerging()) {
    bool done = TryMergePage(root_page_id, &guard, context, parent_index);
    BUSTUB_ASSERT(done, "error in BPlusTree Merge, please allocate more space on bufferpool");
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryMergePage(page_id_t root_page_id, WritePageGuard *guard, Context &context, int parent_index)
    -> bool {
  if (!guard->IsValid()) {
    return true;
  }

  bool valid = true;
  if (context.IsRootPage(root_page_id)) {
    BUSTUB_ASSERT(context.header_page_ != std::nullopt, "TryMergePage context.header_page_ should not be nullopt");

    auto *root_page = guard->AsMut<BPlusTreePage>();
    if (!root_page->IsLeafPage() && root_page->GetSize() == 0) {
      auto *inner_page = guard->AsMut<InternalPage>();
      page_id_t new_root_id = inner_page->ValueAt(0);
      context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
      context.root_page_id_ = new_root_id;

      guard->Drop();
      bpm_->DeletePage(root_page_id);
    }
  } else {
    BUSTUB_ASSERT(!context.write_set_.empty(), "TryMergePage parent not in context.write_set_");

    WritePageGuard parent_guard = std::move(context.write_set_.back());
    context.write_set_.pop_back();
    auto *parent_page = parent_guard.AsMut<InternalPage>();

    page_id_t bro_page_id = INVALID_PAGE_ID;
    if (parent_index == parent_page->GetSize()) {
      bro_page_id = parent_page->ValueAt(parent_index - 1);
      WritePageGuard bro_guard = bpm_->FetchPageWrite(bro_page_id);
      valid = MergeRightSidePage(&bro_guard, guard, &parent_guard, parent_index - 1, bro_page_id);
    } else {
      bro_page_id = parent_page->ValueAt(parent_index + 1);
      WritePageGuard bro_guard = bpm_->FetchPageWrite(bro_page_id);
      valid = MergeRightSidePage(guard, &bro_guard, &parent_guard, parent_index, bro_page_id);
    }

    context.write_set_.push_back(std::move(parent_guard));
  }

  return valid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeRightSidePage(WritePageGuard *guard, WritePageGuard *bro_guard, WritePageGuard *parent_guard,
                                        int index, page_id_t bro_page_id) -> bool {
  auto *parent_page = parent_guard->AsMut<InternalPage>();
  if (!Redistribute(guard, bro_guard, parent_guard, index)) {
    auto *head = guard->AsMut<BPlusTreePage>();

    if (head->IsLeafPage()) {
      auto *page = guard->AsMut<LeafPage>();
      auto *bro_page = bro_guard->AsMut<LeafPage>();

      page->Merge(bro_page, comparator_);
      page->SetNextPageId(bro_page->GetNextPageId());
      parent_page->Remove(index);

      bro_guard->Drop();
      bpm_->DeletePage(bro_page_id);
    } else {
      auto *page = guard->AsMut<InternalPage>();
      auto *bro_page = bro_guard->AsMut<InternalPage>();

      page->Insert(parent_page->KeyAt(index), bro_page->ValueAt(0), comparator_);
      parent_page->Remove(index);
      page->Merge(bro_page, comparator_);

      bro_guard->Drop();
      bpm_->DeletePage(bro_page_id);
    }
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(WritePageGuard *guard, WritePageGuard *bro_guard, WritePageGuard *parent_guard,
                                  int index) -> bool {
  auto *head = guard->AsMut<BPlusTreePage>();
  bool valid = false;

  auto *parent_page = parent_guard->AsMut<InternalPage>();
  if (head->IsLeafPage()) {
    auto *page = guard->AsMut<LeafPage>();
    auto *bro_page = bro_guard->AsMut<LeafPage>();

    if (page->GetSize() < bro_page->GetSize() && bro_page->IsStealable()) {
      MappingType data = bro_page->PopFront();
      page->Insert(data.first, data.second, comparator_);
      parent_page->SetKeyAt(index, bro_page->KeyAt(0));

      valid = true;
    } else if (page->GetSize() > bro_page->GetSize() && page->IsStealable()) {
      MappingType data = page->PopBack();
      bro_page->Insert(data.first, data.second, comparator_);
      parent_page->SetKeyAt(index, bro_page->KeyAt(0));

      valid = true;
    }
  } else {
    auto *page = guard->AsMut<InternalPage>();
    auto *bro_page = bro_guard->AsMut<InternalPage>();
    if (page->GetSize() < bro_page->GetSize() && bro_page->IsStealable()) {
      KeyType key = parent_page->KeyAt(index);
      page->Insert(key, bro_page->ValueAt(0), comparator_);
      parent_page->SetKeyAt(index, bro_page->KeyAt(0));
      bro_page->MoveOn();

      valid = true;
    } else if (page->GetSize() > bro_page->GetSize() && page->IsStealable()) {
      KeyType key = parent_page->KeyAt(index);
      bro_page->Insert(key, bro_page->ValueAt(0), comparator_);
      parent_page->SetKeyAt(index, page->KeyAt(page->GetSize() - 1));
      bro_page->SetValueAt(0, page->ValueAt(page->GetSize()));
      page->PopBack();

      valid = true;
    }
  }

  return valid;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  const auto *root_page = head_guard.As<BPlusTreeHeaderPage>();

  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  ReadPageGuard left_most_guard = bpm_->FetchPageRead(root_page->root_page_id_);
  page_id_t left_most_page_id = root_page->root_page_id_;
  while (!left_most_guard.As<BPlusTreePage>()->IsLeafPage()) {
    auto *inner_page = left_most_guard.As<InternalPage>();

    left_most_page_id = inner_page->ValueAt(0);
    left_most_guard = bpm_->FetchPageRead(left_most_page_id);
  }

  return INDEXITERATOR_TYPE(bpm_, left_most_page_id, std::move(left_most_guard));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  const auto *root_page = head_guard.As<BPlusTreeHeaderPage>();

  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  ReadPageGuard guard = bpm_->FetchPageRead(root_page->root_page_id_);
  page_id_t next_page_id = root_page->root_page_id_;
  const auto *head = guard.As<BPlusTreePage>();
  while (!head->IsLeafPage()) {
    auto *inner_page = guard.As<InternalPage>();
    FindNextPage(key, inner_page, 0, head->GetSize() - 1, &next_page_id);
    guard = bpm_->FetchPageRead(next_page_id);

    head = guard.As<BPlusTreePage>();
  }

  auto *leaf_page = guard.As<LeafPage>();
  int index = 0;
  if (FindKeyIndex(key, leaf_page, 0, leaf_page->GetSize() - 1, &index)) {
    return INDEXITERATOR_TYPE(bpm_, next_page_id, std::move(guard), index);
  }

  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, {}); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  const auto *root_page = guard.As<BPlusTreeHeaderPage>();

  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i <= internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i <= internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

static const bool DEBUG = false;
namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  if (DEBUG) {
    std::cout << "pool_size = " << pool_size << std::endl;
    std::cout << "replacer_k = " << replacer_k << std::endl;
  }

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::FindFrameSlotHepler(frame_id_t *frame_id) -> bool {
  // try to get available frame slot

  bool validate = false;
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    validate = true;
  } else if (replacer_->Evict(frame_id)) {
    Page *page = &pages_[*frame_id];
    // dirty page flush to disk first
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }

    page_table_.erase(page->GetPageId());
    validate = true;
  }

  return validate;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t replacement_frame_id = -1;

  std::unique_lock<std::mutex> lock(latch_);

  if (!FindFrameSlotHepler(&replacement_frame_id)) {
    return nullptr;
  }

  // initial page data and metadata
  Page *page = &pages_[replacement_frame_id];
  page->page_id_ = *page_id = AllocatePage();
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_[*page_id] = replacement_frame_id;

  if (DEBUG) {
    std::cout << "PIN page " << *page_id << std::endl;
  }

  // update replacer metadata
  replacer_->SetEvictable(replacement_frame_id, false);
  replacer_->RecordAccess(replacement_frame_id);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);

    if (DEBUG) {
      std::cout << "PIN page " << page_id << std::endl;
    }

    return &pages_[frame_id];
  }

  frame_id_t replacement_frame_id = -1;
  if (!FindFrameSlotHepler(&replacement_frame_id)) {
    return nullptr;
  }

  // initial page data and metadata
  Page *page = &pages_[replacement_frame_id];
  disk_manager_->ReadPage(page_id, page->GetData());
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_[page_id] = replacement_frame_id;
  if (DEBUG) {
    std::cout << "PIN page " << page_id << std::endl;
  }

  // update replacer metadata
  replacer_->SetEvictable(replacement_frame_id, false);
  replacer_->RecordAccess(replacement_frame_id);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() == 0) {
    return false;
  }

  page->is_dirty_ |= is_dirty;
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  if (DEBUG) {
    std::cout << "UNPIN page " << page_id << std::endl;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lock(latch_);

  for (auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
      continue;
    }

    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  // some thread is processing this page, Forbid deletion !
  if (page->GetPinCount() != 0) {
    return false;
  }

  // free page
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    return {this, page};
  }

  return {nullptr, nullptr};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);

  if (page != nullptr) {
    page->RLatch();
    return {this, page};
  }

  return {nullptr, nullptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);

  if (page != nullptr) {
    page->WLatch();
    return {this, page};
  }

  return {nullptr, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  if (page != nullptr) {
    return {this, page};
  }

  return {nullptr, nullptr};
}
}  // namespace bustub

#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

static const bool DEBUG = false;
namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.page_ = nullptr;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (IsValid()) {
    if (DEBUG) {
      std::cout << "pageId " << PageId() << " Drop" << std::endl;
    }
    bpm_->UnpinPage(PageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();  // release self first then take over other
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  return *this;
}

auto BasicPageGuard::IsValid() -> bool { return page_ != nullptr && bpm_ != nullptr; }

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = BasicPageGuard(std::move(that.guard_)); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }

  if (guard_.IsValid()) {
    guard_.page_->RUnlatch();
  }

  guard_ = std::move(that.guard_);

  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.IsValid()) {
    Page *tmp = guard_.page_;
    guard_.Drop();

    tmp->RUnlatch();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = BasicPageGuard(std::move(that.guard_)); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }

  if (guard_.IsValid()) {
    guard_.page_->WUnlatch();
  }

  guard_ = std::move(that.guard_);

  return *this;
}

auto WritePageGuard::IsValid() -> bool { return guard_.IsValid(); }

void WritePageGuard::Drop() {
  if (guard_.IsValid()) {
    Page *tmp = guard_.page_;
    guard_.Drop();

    tmp->WUnlatch();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub

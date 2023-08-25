//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>
#include <cmath>
#include <cstddef>
#include <memory>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/format.h"

static const bool DEBUG = false;
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  if (DEBUG) {
    std::cout << "k = " << k << std::endl;
    std::cout << "num_frames = " << num_frames << std::endl;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // find largest k-distance backward
  size_t min_least_recent = LONG_LONG_MAX;
  size_t min_k_least_recent = LONG_LONG_MAX;
  bool evict = false;

  latch_.lock();
  for (const auto &store : node_store_) {
    if (store.second->is_evictable_) {
      auto node = store.second;
      size_t least_recent = node->history_.back();
      size_t least_k_recent = node->history_.front();
      if (node->history_.size() < k_ && min_least_recent > least_recent) {
        *frame_id = node->fid_;
        min_least_recent = least_recent;
        min_k_least_recent = 0;
        evict = true;
      } else if (min_k_least_recent > least_k_recent) {
        min_k_least_recent = least_k_recent;
        *frame_id = node->fid_;
        evict = true;
      }
    }
  }

  if (evict) {
    node_store_.erase(*frame_id);
    if (DEBUG) {
      std::cout << "evice frame " << *frame_id << std::endl;
    }
    curr_size_--;
  }
  latch_.unlock();

  return evict;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception(fmt::format("RecordAccess: frame_id {} out of the bound {}", frame_id, replacer_size_));
  }

  latch_.lock();
  if (DEBUG) {
    std::cout << "record frame " << frame_id << std::endl;
  }
  auto iter = node_store_.find(frame_id);
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
  if (iter == node_store_.end()) {
    LRUKNode new_node(frame_id);
    new_node.history_.push_front(current_timestamp_);
    node_store_[frame_id] = std::make_shared<LRUKNode>(new_node);
  } else {
    auto record = iter->second;
    record->history_.push_front(current_timestamp_);
  }

  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception(fmt::format("SetEvictable: frame_id {} out of the bound {}", frame_id, replacer_size_));
  }
  latch_.lock();
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    latch_.unlock();
    return;
  }

  auto record = iter->second;
  if (set_evictable && !record->is_evictable_) {
    record->is_evictable_ = true;
    if (DEBUG) {
      std::cout << "SetEvictable frame " << frame_id << " " << set_evictable << std::endl;
    }
    curr_size_++;
  } else if (!set_evictable && record->is_evictable_) {
    record->is_evictable_ = false;
    if (DEBUG) {
      std::cout << "SetEvictable frame " << frame_id << " " << set_evictable << std::endl;
    }
    curr_size_--;
  }

  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception(fmt::format("Remove: frame_id {} out of the bound {}", frame_id, replacer_size_));
  }

  latch_.lock();

  auto record = node_store_.find(frame_id);
  if (record == node_store_.end()) {
    latch_.unlock();
    return;
  }

  if (!record->second->is_evictable_) {
    throw Exception(fmt::format("Remove: paged[{}] is unevictable", frame_id));
    latch_.unlock();
    return;
  }
  if (DEBUG) {
    std::cout << "remove frame " << frame_id << std::endl;
  }
  node_store_.erase(frame_id);
  curr_size_--;

  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }
}  // namespace bustub

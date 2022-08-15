#include "buffer/lru_replacer.h"

#define DO_LOCK() \
  std::unique_lock<std::mutex> lock { this->latch_ }

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_page_number_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  DO_LOCK();
  if (lru_list_.empty()) {
    return false;
  }

  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  lru_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  DO_LOCK();
  auto iter = lru_map_.find(frame_id);
  if (iter != lru_map_.end()) {
    lru_list_.erase(iter->second);
    lru_map_.erase(iter);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  DO_LOCK();
  auto iter = lru_map_.find(frame_id);
  if (iter == lru_map_.end()) {
    if (max_page_number_ == lru_list_.size()) {
      return;
    }
    lru_list_.push_front(frame_id);
    lru_map_.emplace(frame_id, lru_list_.begin());
  }
}

size_t LRUReplacer::Size() { return this->lru_map_.size(); }

}  // namespace bustub

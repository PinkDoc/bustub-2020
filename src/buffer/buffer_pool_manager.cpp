#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManager::Victim(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.back();
    free_list_.pop_back();
    return true;
  }
  return replacer_->Victim(frame_id);
}

void BufferPoolManager::ChangePage(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
  // It's dirty and need to write to the disk
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  page_table_.erase(page->page_id_);
  if (new_page_id != INVALID_PAGE_ID) {
    page_table_.emplace(new_page_id, new_frame_id);
  }
  page->Reset();
  page->page_id_ = new_page_id;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock<std::mutex> lock{latch_};
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++;
    return page;
  }
  frame_id_t frame_id = -1;
  if (!Victim(&frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[frame_id];
  ChangePage(page, page_id, frame_id);
  disk_manager_->ReadPage(page_id, page->data_);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> lock{latch_};
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  if (page->pin_count_ == 0) {
    return false;
  }

  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::unique_lock<std::mutex> lock{latch_};

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock<std::mutex> lock{latch_};
  frame_id_t frame_id = -1;
  if (!Victim(&frame_id)) {
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();
  Page *page = &pages_[frame_id];
  ChangePage(page, *page_id, frame_id);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;
  assert(page->pin_count_ == 1);
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock<std::mutex> lock{latch_};
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    return false;
  }

  disk_manager_->DeallocatePage(page_id);
  ChangePage(page, -1, frame_id);
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::unique_lock<std::mutex> lock{latch_};
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &pages_[i];
    if (page->page_id_ != -1 && page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
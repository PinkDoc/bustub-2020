/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : buffer_pool_(nullptr), page_(nullptr), index_at_page_(-1), page_id_(INVALID_PAGE_ID) {
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *p, BufferPoolManager *bpm, int idx)
    : buffer_pool_(bpm), page_(p), index_at_page_(idx), page_id_(p->GetPageId()) {
  LOG_DEBUG("Latch node {%d} read", page_->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_) {
    LOG_DEBUG("UnLatch node {%d} read", page_->GetPageId());
    page_->RUnlatch();
    buffer_pool_->UnpinPage(page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (index_at_page_ == -1 || !page_) {
    throw bustub::Exception{ExceptionType::OUT_OF_RANGE, "IndexIterator operator* page_ is nullptr"};
  }

  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_)->GetItem(index_at_page_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }

  LOG_DEBUG("InteratorIndex operator++ index_at_page {%d} tree_page {%d} , page_size {%d} next_page_id {%d}", 
        this->index_at_page_, page_->GetPageId(), reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_)->GetSize(), 
        reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_)->GetNextPageId());

  if (index_at_page_ >= reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_)->GetSize()  - 1) {
    page_->RUnlatch();
    LOG_DEBUG("UnLatch node {%d} read", page_->GetPageId());
    if (reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_)->GetNextPageId() == INVALID_PAGE_ID) {
      page_id_t old_page_id = page_id_;
      page_ = nullptr;
      page_id_ = INVALID_PAGE_ID;
      index_at_page_ = -1;
      buffer_pool_->UnpinPage(old_page_id, false);
    } else {
      auto next_page_id = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_)->GetNextPageId();

      buffer_pool_->UnpinPage(page_->GetPageId(), false);
      auto page = buffer_pool_->FetchPage(next_page_id);
      if (page == nullptr) {
        throw bustub::Exception{ExceptionType::OUT_OF_MEMORY, "IndexIterator operator++ Out Of Memory"};
      }
      page_ = page;
      page_id_ = page_->GetPageId();
      index_at_page_ = 0;

      page_->RLatch();
      LOG_DEBUG("Latch node {%d} read", page_->GetPageId());
    }
  } else {
    index_at_page_++;
  }
  
  

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

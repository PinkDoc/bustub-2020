//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(Page* p, BufferPoolManager* bpm, int idx);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  inline bool operator==(const IndexIterator &itr) const {
    return is_same(itr);
  }

  inline bool operator!=(const IndexIterator &itr) const {
    return !is_same(itr);
  }

 private:

  inline bool is_same(const IndexIterator& itr) const {
    if (page_id_ == INVALID_PAGE_ID && itr.page_id_ == page_id_) return true;
    return page_id_ == itr.page_id_ && index_at_page_ == itr.index_at_page_;
  }

  BufferPoolManager* buffer_pool_;
  Page*page_;
  int index_at_page_;
  page_id_t page_id_;
};

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>



/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  static constexpr int OperatorInsert = 0;
  static constexpr int OperatorDelete = 1;
  static constexpr int OperatorUpdate = 2;
  static constexpr int OperatorFind = 3;

  inline BPlusTreePage* TreePage(Page* p) {
    return reinterpret_cast<BPlusTreePage*>(p->GetData());
  }

  inline LeafPage* AsLeafPage(BPlusTreePage* p) {
    return reinterpret_cast<LeafPage*>(p);
  }

  inline InternalPage * AsInternalPage(BPlusTreePage* p) {
    return reinterpret_cast<InternalPage *>(p);
  }

  inline LeafPage *PageAsLeafPage(Page* p) {
    return AsLeafPage(TreePage(p));
  }

  inline InternalPage *PageAsInternalPage(Page* p) {
    return AsInternalPage(TreePage(p));
  }

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, std::string &outf) {
    /*
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    out << ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
     */
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false, Transaction* t = nullptr, int op = OperatorFind);


 private:
  inline void AddInToDeletePages(Transaction*t, page_id_t p)
  {
    t->AddIntoDeletedPageSet(p );
  }

  inline void DeleteAllOnSet(Transaction* t)
  {
    auto set_ptr = t->GetDeletedPageSet();
    for (auto &i : *set_ptr)
    {
      buffer_pool_manager_->DeletePage(i);
    }
  }

  inline void ReleaseAllLatch(Transaction* t, int op, bool dirty)
  {
      for (auto i : *t->GetPageSet()) {

              if (i == nullptr) {
                switch (op) {
                  case OperatorFind:
                  case OperatorUpdate:
                    root_latch_.RUnlock();
                    break;
                  case OperatorInsert:
                  case OperatorDelete:
                    root_latch_.WUnlock();
                    break;
                }
              } else {
                UnlatchPage(i, op);
              }


          if (i != nullptr) {
              buffer_pool_manager_->UnpinPage(i->GetPageId(), dirty);
          } else {
            LOG_DEBUG("Release root latch");
          }
      }
      t->GetPageSet()->clear();
  }

  inline bool IsSafeOperation(BPlusTreePage* node, int op)
  {
      switch (op) {
          case OperatorFind:
              return true;
          case OperatorUpdate:
              return true;
          case OperatorDelete:
              if (node->IsRootPage()) {
                if (!node->IsLeafPage()) return node->GetSize() > 2;
                else return node->GetSize() > 1;
              } else {
                return node->GetSize() > node->GetMinSize() + 1;
              }
          case OperatorInsert:
              // 如果是node的size 是 MaxSize() -1， 再插入则会分裂
              return node->GetSize() < node->GetMaxSize() - 1;
      }
      return false;
  }

  inline void LatchPage(Page* p, int op)
  {
    switch (op) {
      case OperatorFind:
      case OperatorUpdate:
        p->RLatch();
        break;
      case OperatorDelete:
      case OperatorInsert:
        p->WLatch();
        break;
    }
  }

  inline void UnlatchPage(Page* p , int op)
  {
    switch (op) {
      case OperatorFind:
      case OperatorUpdate:
        p->RUnlatch();
        break;
      case OperatorDelete:
      case OperatorInsert:
        p->WUnlatch();
        break;
    }
  }

  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);


  template <typename N>
  N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, bool on_left,Transaction *transaction = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index, bool on_left);

  bool AdjustRoot(BPlusTreePage *node, Transaction* t);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;

  ReaderWriterLatch root_latch_;
};

}  // namespace bustub

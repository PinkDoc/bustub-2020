//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <string>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/header_page.h"

#include <pthread.h>

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {

  root_latch_.RLock();

  if (IsEmpty()) {
    root_latch_.RUnlock();
    return false;
  }

  if (transaction) transaction->AddIntoPageSet(nullptr);

  auto leaf_page = FindLeafPage(key, false, transaction, OperatorFind);

  ValueType val;
  bool ok = PageAsLeafPage(leaf_page)->Lookup(key, &val, comparator_);

  if (ok) result->push_back(val);

  if (transaction) ReleaseAllLatch(transaction, OperatorFind, false);
  else  {
    leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),  false);
  }

  return ok;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  /**
   * 1. If is empty, new tree
   * 2. Else not empty , find the leaf which contain the Key
   * 3. If left
   * */
  LOG_DEBUG("'Insert'");

  root_latch_.WLock();

  if (IsEmpty()) {
    StartNewTree(key, value);
    root_latch_.WUnlock();
    return true;
  }

  transaction->AddIntoPageSet(nullptr);

  bool ret = InsertIntoLeaf(key, value, transaction);

  ReleaseAllLatch(transaction, OperatorInsert, true);

  return ret;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id = INVALID_PAGE_ID;
  auto page = buffer_pool_manager_->NewPage(&page_id, nullptr);

  LOG_DEBUG("'StartNewTree' page_id %d", page_id);

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "'StartNewTree' BufferPoolManager::NewPage FAIL!");
  }

  root_page_id_ = page_id;
  UpdateRootPageId(0);

  auto leaf_page = AsLeafPage(TreePage(page));
  leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
  leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  leaf_page->SetNextPageId(INVALID_PAGE_ID);

  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_DEBUG("'InsertIntoLeaf'");

  auto page = this->FindLeafPage(key, false, transaction, OperatorInsert);

  if (page == nullptr) {
    LOG_WARN("FindLeafPage page == nullptr");
    return false;
  }

  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType v{};
  if (leaf_node->Lookup(key, &v, comparator_)) {
      // Contain Key
      return false;
  }

  int size = leaf_node->Insert(key, value, comparator_);
  if (size >= leaf_node->GetMaxSize()) {
    LeafPage *new_leaf_node = Split(leaf_node);

    if (new_leaf_node == nullptr) {
      LOG_WARN("'InsertIntoLeaf' Split FAIL BufferPoolManager Out Of Memory");
      return false;
    }

    leaf_node->MoveHalfTo(new_leaf_node);
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf_node->GetPageId());

    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);

    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  }

  page->SetDirty();

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id = INVALID_PAGE_ID;
  Page *page = buffer_pool_manager_->NewPage(&page_id, nullptr);

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "OutOfMemory Split NewPage");
  }

  auto basic_node = reinterpret_cast<BPlusTreePage *>(node);
  if (basic_node->IsLeafPage()) {
    auto new_leaf_node = reinterpret_cast<N *>(page->GetData());
    new_leaf_node->Init(page_id, basic_node->GetParentPageId(), leaf_max_size_);
    new_leaf_node->SetPageType(IndexPageType::LEAF_PAGE);
  } else {
    auto new_internal_node = reinterpret_cast<N *>(page->GetData());
    new_internal_node->Init(page_id, basic_node->GetParentPageId(), internal_max_size_);
    new_internal_node->SetPageType(IndexPageType::INTERNAL_PAGE);
  }

  return reinterpret_cast<N *>(page->GetData());
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  LOG_DEBUG("InsertInToParent old_node page_id{%d} new_node page_id{%d}", old_node->GetPageId(), new_node->GetPageId());
  if (old_node->IsRootPage()) {
    LOG_DEBUG("old_node {%d} Is root page", old_node->GetPageId());
    page_id_t new_root_page_id = INVALID_PAGE_ID;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);

    new_root_page->WLatch();

    if (new_root_page == nullptr) {
      throw bustub::Exception(ExceptionType::OUT_OF_MEMORY, "'InsertInToParent' new_root_page == nullptr");
    }

    auto new_root_internal_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());

    TreePage(new_root_page)->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_internal_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    new_root_internal_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);

    new_root_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }


  auto parent_page_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id, nullptr);

  if (parent_page == nullptr) {
    throw bustub::Exception(ExceptionType::OUT_OF_MEMORY, "'InsertIntoParent' BufferPoolManager::FetchPage FAIL");
  }

  // For debug :) assert(parent_page->IsLocked());

  InternalPage *parent_internal_node = PageAsInternalPage(parent_page);

  int size = parent_internal_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (size >= parent_internal_node->GetMaxSize()) {
    InternalPage *new_parent_internal_node = Split(parent_internal_node);
    parent_internal_node->MoveHalfTo(new_parent_internal_node, buffer_pool_manager_);
    InsertIntoParent(parent_internal_node, new_parent_internal_node->KeyAt(0), new_parent_internal_node, transaction);
    buffer_pool_manager_->UnpinPage(new_parent_internal_node->GetPageId(), true);
  }

  //???????????? ReleaseLatch ?????????
  parent_page->SetDirty();
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistrib ute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {

  LOG_DEBUG("Remove %ld", key.ToString());

  root_latch_.WLock();
  if (IsEmpty()) {
    root_latch_.WUnlock();
    return;
  }

  transaction->GetPageSet()->push_back(nullptr);
  auto page = FindLeafPage(key, false, transaction, OperatorDelete);
  auto leaf_node = PageAsLeafPage(page);

  auto index = leaf_node->KeyIndex(key, comparator_);

  if (index == -1 || comparator_(key, leaf_node->KeyAt(index)) != 0) {
    LOG_DEBUG("Can't find key ");
    ReleaseAllLatch(transaction, OperatorDelete, false);
    return;
  }

  leaf_node->Remove(index);

  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
   CoalesceOrRedistribute(leaf_node, transaction);
  }

  ReleaseAllLatch(transaction, OperatorDelete, true);
  DeleteAllOnSet(transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // root???????????? ??????

  if (reinterpret_cast<BPlusTreePage *>(node)->IsRootPage()) {
    return AdjustRoot(node, transaction);
  }

  auto tree_node = reinterpret_cast<BPlusTreePage*>(node);

  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_internal_node = PageAsInternalPage(parent_page);
  if (parent_page == nullptr) {
    throw Exception{ExceptionType::OUT_OF_MEMORY, "CoalesceOrRedistribute Out Of Memory at fetching parent_page"};
  }

  auto neighbor_page_id = INVALID_PAGE_ID;
  KeyType mid_key{};
  int index = -1;
  auto on_left = PageAsInternalPage(parent_page)->GetSibling(node->GetPageId(), &neighbor_page_id,  &mid_key,  &index);

  assert(index >= 0);

  auto neighbor_page = buffer_pool_manager_->FetchPage(neighbor_page_id);

  if (neighbor_page == nullptr) {
    throw bustub::Exception{ExceptionType::OUT_OF_MEMORY, "CoalesceOrRedistribute Out Of Memory at fetching neighbor_page"};
  }

  auto neighbor_node = TreePage(neighbor_page);
  if (TreePage(neighbor_page)->GetSize() + tree_node->GetSize() < tree_node->GetMaxSize()) {
    Coalesce(&neighbor_node, &tree_node, &parent_internal_node, index, on_left, transaction);
    if (on_left) {
      buffer_pool_manager_->UnpinPage(neighbor_page_id, true);
    }
    return on_left;
  } else {
    Redistribute(neighbor_node, tree_node, index, on_left);
    buffer_pool_manager_->UnpinPage(neighbor_page_id, true);
  }

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              bool on_left, Transaction *transaction) {
  LOG_DEBUG("Coalesce neighbor_node {%d} node {%d} parent {%d}",(*neighbor_node)->GetPageId(), (*node)->GetPageId(), (*parent)->GetPageId());
  auto tree_node = reinterpret_cast<BPlusTreePage *>(*node);
  auto sibling_node = reinterpret_cast<BPlusTreePage *>(*neighbor_node);
  auto parent_internal_node = reinterpret_cast<InternalPage *>(*parent);

  if (tree_node->IsLeafPage()) {
    if (on_left) {
      AsLeafPage(tree_node)->MoveAllTo(AsLeafPage(sibling_node));
      AsLeafPage(sibling_node)->SetNextPageId(AsLeafPage(tree_node)->GetNextPageId());
      AddInToDeletePages(transaction, tree_node->GetPageId());
    } else {
      AsLeafPage(sibling_node)->MoveAllTo(AsLeafPage(tree_node));
      AsLeafPage(tree_node)->SetNextPageId(AsLeafPage(sibling_node)->GetNextPageId());
      AddInToDeletePages(transaction, sibling_node->GetPageId());
    }
  } else {
    if (on_left) {
      AsInternalPage(tree_node)->MoveAllTo(AsInternalPage(sibling_node), parent_internal_node->KeyAt(index),
                                           buffer_pool_manager_);
      AddInToDeletePages(transaction, tree_node->GetPageId());
    } else {
      AsInternalPage(sibling_node)
          ->MoveAllTo(AsInternalPage(tree_node), parent_internal_node->KeyAt(index), buffer_pool_manager_);
      AddInToDeletePages(transaction, sibling_node->GetPageId());
    }
  }

  parent_internal_node->Remove(index);

  bool ret = false;

  if (parent_internal_node->GetSize() < parent_internal_node->GetMinSize()) {
    ret = CoalesceOrRedistribute(parent_internal_node, transaction);
  }

  return ret;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index, bool on_left) {
  LOG_DEBUG("Redistribute neighbor_node {%d} node {%d} ",(neighbor_node)->GetPageId(), (node)->GetPageId());

  auto sibling_node = reinterpret_cast<BPlusTreePage *>(neighbor_node);
  auto tree_node = reinterpret_cast<BPlusTreePage *>(node);
  auto parent_page = buffer_pool_manager_->FetchPage(tree_node->GetParentPageId());

  if (parent_page == nullptr) {
    throw bustub::Exception{ExceptionType::OUT_OF_MEMORY, "Out Of Memory Redistribute Fetch parent's page"};
  }

  auto parent_internal_node = PageAsInternalPage(parent_page);

  if (tree_node->IsLeafPage()) {
    if (on_left) {
      AsLeafPage(sibling_node)->MoveLastToFrontOf(AsLeafPage(tree_node));
      auto idx =index;
      parent_internal_node->SetKeyAt(idx, AsLeafPage(tree_node)->KeyAt(0));
    } else {
      AsLeafPage(sibling_node)->MoveFirstToEndOf(AsLeafPage(tree_node));
      auto idx = index;
      parent_internal_node->SetKeyAt(idx, AsLeafPage(sibling_node)->KeyAt(0));
    }

  } else {
    if (on_left) {
      auto idx = index;
      auto mid_key = parent_internal_node->KeyAt(idx);
      auto new_mid_key = AsInternalPage(sibling_node)->KeyAt(sibling_node->GetSize() - 1);
      AsInternalPage(sibling_node)->MoveLastToFrontOf(AsInternalPage(tree_node), mid_key, buffer_pool_manager_);
      parent_internal_node->SetKeyAt(idx, new_mid_key);
    } else {
      auto idx = index;
      auto mid_key = parent_internal_node->KeyAt(idx);
      auto new_mid_key = AsInternalPage(sibling_node)->KeyAt(1);
      AsInternalPage(sibling_node)->MoveFirstToEndOf(AsInternalPage(tree_node), mid_key, buffer_pool_manager_);
      parent_internal_node->SetKeyAt(idx, new_mid_key);
    }
  }

  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction* t) {
    if (old_root_node->GetSize() > 1){
        return false;
    }

    page_id_t child_page_id;
    if (old_root_node->IsLeafPage()){
        if (old_root_node->GetSize() == 1){
            return false;
        }
        // ?????????0
        t->AddIntoDeletedPageSet(old_root_node->GetPageId());
        child_page_id = INVALID_PAGE_ID;
    } else {
      // ??????????????????????????????????????????internal
        auto old_root_internal_node = AsInternalPage(old_root_node);
        child_page_id = old_root_internal_node->RemoveAndReturnOnlyChild();
        auto new_root_page_ptr = buffer_pool_manager_->FetchPage(child_page_id);
        auto new_root_ptr = PageAsInternalPage(new_root_page_ptr);
        new_root_ptr->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(child_page_id, true);
        t->AddIntoDeletedPageSet(old_root_internal_node->GetPageId());
    }
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);

    return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType k{};
  root_latch_.RLock();
  auto page = FindLeafPage(k, true, nullptr, OperatorFind);

  if (page == nullptr) {
    throw bustub::Exception{ExceptionType::OUT_OF_MEMORY, "Out Of Memory Begin()"};
  }
  return INDEXITERATOR_TYPE{page, buffer_pool_manager_, 0};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  root_latch_.RLock();
  auto page = FindLeafPage(key, false);

  if (page == nullptr) {
    throw bustub::Exception{ExceptionType::OUT_OF_MEMORY, "Out Of Memory Begin(const KeyType& key)"};
  }

  auto idx =  PageAsLeafPage(page)->KeyIndex(key, comparator_);
  if (idx == -1) {
    page->RUnlatch();
    return end();
  }

  return INDEXITERATOR_TYPE{page, buffer_pool_manager_, idx};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Transaction* t, int op) {
  if (IsEmpty()) {
    return nullptr;
  }

  Page* page = nullptr,* last_page = nullptr;
  auto page_id = root_page_id_, last_page_id = INVALID_PAGE_ID;

  while (true)
  {
    page = buffer_pool_manager_->FetchPage(page_id);
    if (!page) {
      throw Exception{ExceptionType::OUT_OF_MEMORY, "FindLeafPage Out Of Memory"};
    }

    LatchPage(page, op);
    if (IsSafeOperation(TreePage(page), op)) {
      if (t) {
        ReleaseAllLatch(t, op, false);
        t->AddIntoPageSet(page);
      } else {
        if (page_id == root_page_id_)
          root_latch_.RUnlock();
        else
        {
          UnlatchPage(last_page, op);
          buffer_pool_manager_->UnpinPage(last_page_id, false);
        }
      }
    } else {
      t->AddIntoPageSet(page);
    }

    // ?????? ??????????????????????????????????????????
    // ?????? ?????????page???????????????
    last_page_id = page_id;
    last_page = page;

    if (TreePage(page)->IsLeafPage()) {
      break;
    }

    if (leftMost) {
      page_id = PageAsInternalPage(page)->ValueAt(0);
    } else {
      page_id = PageAsInternalPage(page)->Lookup(key, comparator_);
    }

  }

  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

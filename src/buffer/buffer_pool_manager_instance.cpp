//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/logger.h"
#include "common/macros.h"
namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  for (size_t i = 0; i < pool_size_; ++i) {
    pages_[i].~Page();
  }
  free_list_.clear();
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  this->disk_manager_->WritePage(pages_[it->second].page_id_, pages_[it->second].data_);
  // pages_[it->second].is_dirty_ = false;
  latch_.unlock();

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; ++i) {
    this->FlushPage(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  frame_id_t fid = 0;

  latch_.lock();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (this->replacer_->Size() == 0 && free_list_.empty()) {
    LOG_INFO("all pages are pinned\n");
    *page_id = INVALID_PAGE_ID;
    latch_.unlock();
    return nullptr;
  }

  page_id_t pid = this->AllocatePage();

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // TODO pick from free list
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    // LOG_INFO("new page from free list page_id=%d,fid=%d\n", *page_id, fid);
  } else {
    if (!this->replacer_->Victim(&fid)) {
      *page_id = INVALID_PAGE_ID;
      latch_.unlock();
      return nullptr;
    }

    // LOG_INFO("new page from victim page_id=%d,fid=%d\n", *page_id, fid);
  }

  Page *p = &(pages_)[fid];
  page_table_.erase(p->page_id_);

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  p->ResetMemory();
  p->page_id_ = pid;
  p->pin_count_++;
  page_table_[pid] = fid;

  LOG_INFO("new page pid=%d,fid=%d\n", pid, fid);

  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = pid;

  latch_.unlock();

  return p;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  latch_.lock();
  Page *p = nullptr;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    p = &(pages_)[it->second];
    p->pin_count_++;
    this->replacer_->Pin(it->second);
    latch_.unlock();
    LOG_INFO("fetch page from page table page_id=%d, fid=%d\n", p->page_id_, it->second);
    return p;
  }

  frame_id_t fid = 0;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!this->replacer_->Victim(&fid)) {
      LOG_INFO("replacer find victim err page_id=%d, fid=%d\n", page_id, fid);
      latch_.unlock();
      return nullptr;
    }

    LOG_INFO("frame_id=%d is victim\n", fid);
  }

  // 2.     If R is dirty, write it back to the disk.
  LOG_INFO("fetch page, r is dirty page, fid=%d,page_id=%d\n", fid, page_id);
  Page *r = &(pages_)[fid];
  LOG_INFO("fetch page, r is dirty page, fid=%d,r.page_id=%d,page_id=%d\n", fid, r->page_id_, page_id);
  if (r->IsDirty()) {
    this->disk_manager_->WritePage(r->page_id_, r->data_);
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(r->GetPageId());
  p = r;
  p->page_id_ = page_id;
  p->pin_count_++;
  page_table_[p->GetPageId()] = fid;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  this->disk_manager_->ReadPage(p->page_id_, p->data_);

  latch_.unlock();

  return p;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  latch_.lock();
  // 0.   Make sure you call DeallocatePage!
  this->DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  auto p = &pages_[it->second];
  if (p->GetPinCount()) {
    latch_.unlock();
    return false;
  }

  page_table_.erase(page_id);
  p->ResetMemory();
  p->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(it->second);

  latch_.unlock();

  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  Page *p = &pages_[it->second];
  if (p->GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }

  if (is_dirty) {
    this->disk_manager_->WritePage(p->page_id_, p->data_);
  }
  this->replacer_->Unpin(it->second);
  p->pin_count_--;
  latch_.unlock();

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub

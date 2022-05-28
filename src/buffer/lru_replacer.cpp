//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { cap = num_pages; }

LRUReplacer::~LRUReplacer() {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (que.size() == 0) {
    return false;
  }
  *frame_id = que.front();
  mu.lock();
  que.pop_front();
  que_page.erase(*frame_id);
  mu.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  for (auto it = que.begin(); it != que.end(); it++) {
    if (*it == frame_id) {
      mu.lock();
      que.erase(it);
      pined_page[frame_id] = true;
      que_page.erase(frame_id);
      mu.unlock();
      break;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mu.lock();
  pined_page.erase(frame_id);
  if (que_page[frame_id] == false) {
    que_page[frame_id] = true;
    que.push_back(frame_id);
  }
  mu.unlock();
}

auto LRUReplacer::Size() -> size_t { return que.size(); }

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) { cap = num_pages; }

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (que.size() == 0) {
    return false;
  }
  mu.lock();
  *frame_id = que.front();
  que.pop_front();
  que_page.erase(*frame_id);
  mu.unlock();
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
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

void ClockReplacer::Unpin(frame_id_t frame_id) {
  mu.lock();
  pined_page.erase(frame_id);
  if (que_page[frame_id] == false) {
    que_page[frame_id] = true;
    que.push_back(frame_id);
  }
  mu.unlock();
}

size_t ClockReplacer::Size() { return que.size(); }

}  // namespace bustub

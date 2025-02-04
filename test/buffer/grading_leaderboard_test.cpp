/**
 * grading_leaderboard_test.cpp
 *
 * THIS TEST WILL NOT BE RUN ON GRADESCOPE
 *
 * Test for calculating the milliseconds time for running
 * NewPage, UnpinPage, FetchPage, DeletePage in Buffer Pool Manager.
 */

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer_test.cpp
//
// Identification: test/buffer/leaderboard_test.cpp
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
// #include "storage/disk/disk_manager_memory.h"

namespace bustub {

TEST(LeaderboardTest, Time) {
  const size_t buffer_pool_size = 15000;

  // DiskManagerMemory *dm = new DiskManagerMemory();
  auto *dm = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(buffer_pool_size, dm, 100);
  page_id_t temp;
  for (size_t i = 0; i < buffer_pool_size; i++) {
    LOG_DEBUG("new page");
    bpm->NewPage(&temp);
  }
  for (size_t i = 0; i < buffer_pool_size; i++) {
    LOG_DEBUG("unpin page");
    bpm->UnpinPage(i, false);
    bpm->FetchPage(i);
    bpm->UnpinPage(i, false);
  }
  for (size_t i = buffer_pool_size - 1; i != 0; i--) {
    LOG_DEBUG("delete page %zu", i);
    bpm->DeletePage(i);
    bpm->NewPage(&temp);
    bpm->UnpinPage(temp, false);
    bpm->DeletePage(temp);
    bpm->NewPage(&temp);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  dm->ShutDown();
  remove("test.db");
  remove("test.log");

  delete bpm;
  delete dm;
}

}  // namespace bustub

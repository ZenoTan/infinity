/**
 * Examples - Read/Write/Send Operations
 *
 * (c) 2018 Claude Barthels, ETH Zurich
 * Contact: claudeb@inf.ethz.ch
 *
 */

#include <cassert>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <thread>
#include <vector>

#include <infinity/core/Context.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/requests/RequestToken.h>

#define PORT_NUMBER 8011
#define SERVER_IP "155.198.152.17"

#define NUM_ITER 100
#define NUM_REQ 1000
#define REQ_LIST 100
#define REQ_BYTES 512

#define NUM_THREAD 16

uint64_t timeDiff(struct timeval stop, struct timeval start) {
  return (stop.tv_sec * 1000000L + stop.tv_usec) -
         (start.tv_sec * 1000000L + start.tv_usec);
}

void run_server(int rank) {
  infinity::core::Context *context = new infinity::core::Context();
  infinity::queues::QueuePairFactory *qpFactory =
      new infinity::queues::QueuePairFactory(context);
  infinity::queues::QueuePair *qp;
  printf("Creating buffers to read from and write to\n");
  infinity::memory::Buffer *bufferToReadWrite = new infinity::memory::Buffer(
      context, REQ_LIST * REQ_BYTES * sizeof(char));
  infinity::memory::RegionToken *bufferToken =
      bufferToReadWrite->createRegionToken();

  printf("Creating buffers to receive a message\n");
  infinity::memory::Buffer *bufferToReceive =
      new infinity::memory::Buffer(context, 128 * sizeof(char));
  context->postReceiveBuffer(bufferToReceive);

  printf("Setting up connection (blocking)\n");
  qpFactory->bindToPort(PORT_NUMBER + rank);
  qp = qpFactory->acceptIncomingConnection(
      bufferToken, sizeof(infinity::memory::RegionToken));

  printf("Waiting for message (blocking)\n");
  infinity::core::receive_element_t receiveElement;
  while (!context->receive(&receiveElement))
    ;

  printf("Message received\n");
  delete bufferToReadWrite;
  delete bufferToReceive;

  delete qp;
  delete qpFactory;
  delete context;
}

void run_client(int rank) {
  infinity::core::Context *context = new infinity::core::Context();
  infinity::queues::QueuePairFactory *qpFactory =
      new infinity::queues::QueuePairFactory(context);
  infinity::queues::QueuePair *qp;
  printf("Connecting to remote node\n");
  qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT_NUMBER);
  infinity::memory::RegionToken *remoteBufferToken =
      (infinity::memory::RegionToken *)qp->getUserData();

  printf("Creating buffers\n");
  std::vector<infinity::memory::Buffer *> buffers;
  infinity::memory::Buffer *buffer1Sided = new infinity::memory::Buffer(
      context, REQ_LIST * REQ_BYTES * sizeof(char));

  infinity::queues::SendRequestBuffer send_buffer(REQ_LIST);
  std::vector<uint64_t> local_offset(REQ_LIST, 0);
  std::vector<uint64_t> remote_offset(REQ_LIST, 0);
  for (int i = 0; i < REQ_LIST; i++) {
    local_offset[i] = i * REQ_BYTES;
    remote_offset[i] = i * REQ_BYTES;
  }

  printf("Reading content from remote buffer\n");
  std::vector<infinity::requests::RequestToken *> requests;
  for (int i = 0; i < NUM_REQ; i++) {
    requests.push_back(new infinity::requests::RequestToken(context));
  }
  uint64_t cnt = 0;
  ibv_wc wc;
  for (int i = 0; i < NUM_ITER; i++) {
    struct timeval start;
    struct timeval stop;
    uint64_t cur = 0;
    gettimeofday(&start, NULL);
    for (int j = 0; j < NUM_REQ; j++) {
      qp->batch_read(buffer1Sided, local_offset, remoteBufferToken,
                     remote_offset, REQ_BYTES,
                     infinity::queues::OperationFlags(), requests[j],
                     send_buffer);
      context->pollSendCompletionFast(1, &wc);
    }
    gettimeofday(&stop, NULL);
    cur = timeDiff(stop, start);

    printf("Cur %d took %lu\n", REQ_BYTES * NUM_REQ * REQ_LIST, cur);
    cnt += cur;
  }

  printf("Avg %d took %lu\n", REQ_BYTES * NUM_REQ * REQ_LIST, cnt / NUM_ITER);

  // printf("Writing content to remote buffer\n");
  // qp->write(buffer1Sided, remoteBufferToken, &requestToken);
  // requestToken.waitUntilCompleted();

  printf("Sending message to remote host\n");
  infinity::requests::RequestToken requestToken(context);
  infinity::memory::Buffer *buffer2Sided =
      new infinity::memory::Buffer(context, 128 * sizeof(char));
  qp->send(buffer2Sided, &requestToken);
  requestToken.waitUntilCompleted();

  delete buffer1Sided;
  delete buffer2Sided;

  delete qp;
  delete qpFactory;
  delete context;
}

// Usage: ./progam -s for server and ./program for client component
int main(int argc, char **argv) {

  bool isServer = false;

  while (argc > 1) {
    if (argv[1][0] == '-') {
      switch (argv[1][1]) {

      case 's': {
        isServer = true;
        break;
      }
      }
    }
    ++argv;
    --argc;
  }

  if (isServer) {
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREAD; i++) {
      threads.push_back(std::thread(run_server, i));
    }
    for (auto &t : threads) {
      t.join();
    }
  } else {
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREAD; i++) {
      threads.push_back(std::thread(run_client, i));
    }
    for (auto &t : threads) {
      t.join();
    }
  }

  // infinity::core::Context *context = new infinity::core::Context();
  // infinity::queues::QueuePairFactory *qpFactory =
  //     new infinity::queues::QueuePairFactory(context);
  // infinity::queues::QueuePair *qp;

  // if (isServer) {

  //   printf("Creating buffers to read from and write to\n");
  //   infinity::memory::Buffer *bufferToReadWrite = new
  //   infinity::memory::Buffer(
  //       context, REQ_LIST * REQ_BYTES * sizeof(char));
  //   infinity::memory::RegionToken *bufferToken =
  //       bufferToReadWrite->createRegionToken();

  //   printf("Creating buffers to receive a message\n");
  //   infinity::memory::Buffer *bufferToReceive =
  //       new infinity::memory::Buffer(context, 128 * sizeof(char));
  //   context->postReceiveBuffer(bufferToReceive);

  //   printf("Setting up connection (blocking)\n");
  //   qpFactory->bindToPort(PORT_NUMBER);
  //   qp = qpFactory->acceptIncomingConnection(
  //       bufferToken, sizeof(infinity::memory::RegionToken));

  //   printf("Waiting for message (blocking)\n");
  //   infinity::core::receive_element_t receiveElement;
  //   while (!context->receive(&receiveElement))
  //     ;

  //   printf("Message received\n");
  //   delete bufferToReadWrite;
  //   delete bufferToReceive;

  // } else {

  //   printf("Connecting to remote node\n");
  //   qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT_NUMBER);
  //   infinity::memory::RegionToken *remoteBufferToken =
  //       (infinity::memory::RegionToken *)qp->getUserData();

  //   printf("Creating buffers\n");
  //   std::vector<infinity::memory::Buffer *> buffers;
  //   infinity::memory::Buffer *buffer1Sided = new infinity::memory::Buffer(
  //       context, REQ_LIST * REQ_BYTES * sizeof(char));

  //   infinity::queues::SendRequestBuffer send_buffer(REQ_LIST);
  //   std::vector<uint64_t> local_offset(REQ_LIST, 0);
  //   std::vector<uint64_t> remote_offset(REQ_LIST, 0);
  //   for (int i = 0; i < REQ_LIST; i++) {
  //     local_offset[i] = i * REQ_BYTES;
  //     remote_offset[i] = i * REQ_BYTES;
  //   }

  //   printf("Reading content from remote buffer\n");
  //   std::vector<infinity::requests::RequestToken *> requests;
  //   for (int i = 0; i < NUM_REQ; i++) {
  //     requests.push_back(new infinity::requests::RequestToken(context));
  //   }
  //   uint64_t cnt = 0;
  //   ibv_wc wc;
  //   for (int i = 0; i < NUM_ITER; i++) {
  //     struct timeval start;
  //     struct timeval stop;
  //     uint64_t cur = 0;
  //     gettimeofday(&start, NULL);
  //     for (int j = 0; j < NUM_REQ; j++) {
  //       qp->batch_read(buffer1Sided, local_offset, remoteBufferToken,
  //                      remote_offset, REQ_BYTES,
  //                      infinity::queues::OperationFlags(), requests[j],
  //                      send_buffer);
  //       context->pollSendCompletionFast(1, &wc);
  //     }
  //     gettimeofday(&stop, NULL);
  //     cur = timeDiff(stop, start);

  //     printf("Cur %d took %lu\n", REQ_BYTES * NUM_REQ * REQ_LIST, cur);
  //     cnt += cur;
  //   }

  //   printf("Avg %d took %lu\n", REQ_BYTES * NUM_REQ * REQ_LIST, cnt /
  //   NUM_ITER);

  //   // printf("Writing content to remote buffer\n");
  //   // qp->write(buffer1Sided, remoteBufferToken, &requestToken);
  //   // requestToken.waitUntilCompleted();

  //   printf("Sending message to remote host\n");
  //   infinity::requests::RequestToken requestToken(context);
  //   infinity::memory::Buffer *buffer2Sided =
  //       new infinity::memory::Buffer(context, 128 * sizeof(char));
  //   qp->send(buffer2Sided, &requestToken);
  //   requestToken.waitUntilCompleted();

  //   delete buffer1Sided;
  //   delete buffer2Sided;
  // }

  // delete qp;
  // delete qpFactory;
  // delete context;

  // return 0;
}

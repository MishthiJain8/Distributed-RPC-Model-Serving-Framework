#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "serving.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using serving::InferenceRequest;
using serving::InferenceResponse;
using serving::OrchestrationService;

static std::string MakeRequestId(int id) {
  return "req-" + std::to_string(id);
}

static std::vector<float> GeneratePayload(size_t size) {
  std::vector<float> values(size);
  static std::mt19937_64 rng(std::random_device{}());
  static std::uniform_real_distribution<float> dist(0.0f, 1.0f);
  for (auto& v : values) {
    v = dist(rng);
  }
  return values;
}

int main(int argc, char** argv) {
  std::string orchestrator_address = "localhost:50051";
  if (argc > 1) {
    orchestrator_address = argv[1];
  }

  auto channel = grpc::CreateChannel(orchestrator_address, grpc::InsecureChannelCredentials());
  auto stub = OrchestrationService::NewStub(channel);

  const int total_requests = 80;
  const int concurrency = 10;
  const int payload_size = 64;

  std::vector<std::future<void>> futures;
  std::atomic<int> completed{0};

  std::cout << "[Client] Sending " << total_requests << " requests to " << orchestrator_address << " with " << concurrency << " concurrent workers\n";

  for (int thread_id = 0; thread_id < concurrency; ++thread_id) {
    futures.push_back(std::async(std::launch::async, [&, thread_id]() {
      for (int i = thread_id; i < total_requests; i += concurrency) {
        InferenceRequest request;
        request.set_request_id(MakeRequestId(i));
        request.set_client_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        auto payload = GeneratePayload(payload_size);
        for (float value : payload) {
          request.add_values(value);
        }

        ClientContext context;
        InferenceResponse response;
        grpc::Status status = stub->Infer(&context, request, &response);
        if (!status.ok()) {
          std::cerr << "[Client] Request " << request.request_id() << " failed: " << status.error_message() << "\n";
        } else {
          std::cout << "[Client] Received response for " << response.request_id() << " status=" << response.status() << " worker=" << response.worker_id() << "\n";
        }
        ++completed;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }));
  }

  for (auto& future : futures) {
    future.wait();
  }

  std::cout << "[Client] Completed " << completed.load() << " requests\n";
  return 0;
}

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "serving.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Channel;

using serving::BatchRequest;
using serving::BatchResponse;
using serving::HeartbeatAck;
using serving::HeartbeatMessage;
using serving::InferenceRequest;
using serving::InferenceResponse;
using serving::WorkerHeartbeat;
using serving::WorkerNode;
using serving::OrchestrationService;
using serving::WorkerRegistration;
using serving::WorkerStatus;

static int64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

struct InferenceJob {
  InferenceRequest request;
  int64_t enqueue_time_ms;
  std::shared_ptr<std::promise<InferenceResponse>> promise;
};

template <typename T>
class LockFreeMPSCQueue {
 public:
  struct Node {
    std::optional<T> payload;
    std::atomic<Node*> next{nullptr};
    Node() = default;
    explicit Node(T&& value) : payload(std::move(value)) {}
  };

  LockFreeMPSCQueue() {
    Node* dummy = new Node();
    head_.store(dummy, std::memory_order_relaxed);
    tail_.store(dummy, std::memory_order_relaxed);
  }

  ~LockFreeMPSCQueue() {
    Node* node = tail_.load(std::memory_order_relaxed);
    while (node != nullptr) {
      Node* next = node->next.load(std::memory_order_relaxed);
      delete node;
      node = next;
    }
  }

  void enqueue(T value) {
    Node* node = new Node(std::move(value));
    node->next.store(nullptr, std::memory_order_relaxed);
    Node* prev = head_.exchange(node, std::memory_order_acq_rel);
    prev->next.store(node, std::memory_order_release);
  }

  std::optional<T> dequeue() {
    Node* tail = tail_.load(std::memory_order_relaxed);
    Node* next = tail->next.load(std::memory_order_acquire);
    if (next == nullptr) {
      return std::nullopt;
    }
    std::optional<T> result = std::move(next->payload);
    tail_.store(next, std::memory_order_release);
    delete tail;
    return result;
  }

  bool empty() const {
    Node* tail = tail_.load(std::memory_order_relaxed);
    return tail->next.load(std::memory_order_acquire) == nullptr;
  }

 private:
  std::atomic<Node*> head_;
  std::atomic<Node*> tail_;
};

struct WorkerInfo {
  std::string worker_id;
  std::string address;
  std::atomic<int> active_tasks{0};
  std::atomic<int64_t> last_heartbeat_ms{0};
  std::shared_ptr<WorkerNode::Stub> stub;
};

class WorkerRegistry {
 public:
  void updateRegistration(const WorkerRegistration& reg) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = workers_.find(reg.worker_id());
    if (it == workers_.end()) {
      std::cout << "[Orchestrator] Registering worker " << reg.worker_id() << " @ " << reg.worker_address() << "\n";
      auto info = std::make_shared<WorkerInfo>();
      info->worker_id = reg.worker_id();
      info->address = reg.worker_address();
      info->last_heartbeat_ms.store(TimestampMs(), std::memory_order_relaxed);
      info->stub = WorkerNode::NewStub(grpc::CreateChannel(reg.worker_address(), grpc::InsecureChannelCredentials()));
      workers_.emplace(reg.worker_id(), info);
    } else {
      it->second->address = reg.worker_address();
      it->second->last_heartbeat_ms.store(TimestampMs(), std::memory_order_relaxed);
    }
  }

  void updateStatus(const WorkerStatus& status) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = workers_.find(status.worker_id());
    if (it != workers_.end()) {
      it->second->active_tasks.store(status.active_tasks(), std::memory_order_relaxed);
      it->second->last_heartbeat_ms.store(status.timestamp_ms(), std::memory_order_relaxed);
    }
  }

  std::shared_ptr<WorkerInfo> chooseWorker() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::shared_ptr<WorkerInfo> best = nullptr;
    for (auto& [id, worker] : workers_) {
      if (!worker || !worker->stub) {
        continue;
      }
      if (!best || worker->active_tasks.load(std::memory_order_relaxed) < best->active_tasks.load(std::memory_order_relaxed)) {
        best = worker;
      }
    }
    return best;
  }

  void pruneExpired(int64_t expiry_ms) {
    std::lock_guard<std::mutex> guard(mutex_);
    int removed = 0;
    for (auto it = workers_.begin(); it != workers_.end();) {
      if (TimestampMs() - it->second->last_heartbeat_ms.load(std::memory_order_relaxed) > expiry_ms) {
        std::cout << "[Orchestrator] Worker " << it->second->worker_id << " heartbeat expired, removing from registry\n";
        it = workers_.erase(it);
        removed++;
      } else {
        ++it;
      }
    }
    if (removed > 0) {
      std::cout << "[Orchestrator] Pruned " << removed << " dead workers" << std::endl;
    }
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<WorkerInfo>> workers_;
};

class OrchestratorServiceImpl final : public OrchestrationService::Service {
 public:
  OrchestratorServiceImpl(LockFreeMPSCQueue<InferenceJob>& queue, std::condition_variable& cv, std::atomic<bool>& running)
      : request_queue_(queue), batch_cv_(cv), running_(running) {}

  Status Infer(ServerContext* context, const InferenceRequest* request, InferenceResponse* response) override {
    auto promise = std::make_shared<std::promise<InferenceResponse>>();
    std::future<InferenceResponse> future = promise->get_future();

    InferenceJob job;
    job.request = *request;
    job.enqueue_time_ms = TimestampMs();
    job.promise = promise;
    request_queue_.enqueue(std::move(job));
    batch_cv_.notify_one();

    std::cout << "[Orchestrator] Accepted request " << request->request_id() << " for batching\n";

    auto status = future.wait_for(std::chrono::seconds(10));
    if (status == std::future_status::ready) {
      *response = future.get();
      return Status::OK;
    }

    response->set_request_id(request->request_id());
    response->set_status("TIMEOUT");
    return Status::OK;
  }

 private:
  LockFreeMPSCQueue<InferenceJob>& request_queue_;
  std::condition_variable& batch_cv_;
  std::atomic<bool>& running_;
};

class WorkerHeartbeatServiceImpl final : public WorkerHeartbeat::Service {
 public:
  explicit WorkerHeartbeatServiceImpl(WorkerRegistry& registry) : registry_(registry) {}

  Status Heartbeat(ServerContext* context, ServerReaderWriter<HeartbeatAck, HeartbeatMessage>* stream) override {
    HeartbeatMessage msg;
    while (stream->Read(&msg)) {
      if (msg.has_registration()) {
        registry_.updateRegistration(msg.registration());
        HeartbeatAck ack;
        ack.set_accepted(true);
        ack.set_message("worker registered");
        stream->Write(ack);
      } else if (msg.has_status()) {
        registry_.updateStatus(msg.status());
        HeartbeatAck ack;
        ack.set_accepted(true);
        ack.set_message("status updated");
        stream->Write(ack);
      }
    }
    return Status::OK;
  }

 private:
  WorkerRegistry& registry_;
};

class OrchestratorDaemon {
 public:
  OrchestratorDaemon(std::string listen_address)
      : listen_address_(std::move(listen_address)), running_(true) {}

  void run() {
    OrchestratorServiceImpl orchestration_service(request_queue_, batch_cv_, running_);
    WorkerHeartbeatServiceImpl heartbeat_service(worker_registry_);

    ServerBuilder builder;
    builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(&orchestration_service);
    builder.RegisterService(&heartbeat_service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server) {
      std::cerr << "[Orchestrator] Failed to start server on " << listen_address_ << std::endl;
      return;
    }
    std::cout << "[Orchestrator] Listening on " << listen_address_ << std::endl;

    std::jthread batch_thread(&OrchestratorDaemon::batchLoop, this);
    std::jthread health_thread(&OrchestratorDaemon::healthLoop, this);

    server->Wait();
    running_.store(false);
    batch_cv_.notify_all();
  }

 private:
  void batchLoop() {
    const size_t max_batch_size = 16;
    const std::chrono::milliseconds max_wait{20};

    while (running_.load(std::memory_order_relaxed)) {
      std::vector<InferenceJob> batch;
      batch.reserve(max_batch_size);
      auto window_deadline = std::chrono::steady_clock::now() + max_wait;

      while (batch.size() < max_batch_size) {
        if (auto job = request_queue_.dequeue()) {
          batch.push_back(std::move(*job));
          if (batch.size() >= max_batch_size) {
            break;
          }
        } else {
          std::unique_lock<std::mutex> lock(batch_mutex_);
          batch_cv_.wait_until(lock, window_deadline, [this] {
            return !request_queue_.empty() || !running_.load(std::memory_order_relaxed);
          });
          if (!running_.load(std::memory_order_relaxed) && request_queue_.empty()) {
            break;
          }
          if (std::chrono::steady_clock::now() >= window_deadline) {
            break;
          }
        }
      }

      if (batch.empty()) {
        continue;
      }

      dispatchBatch(std::move(batch));
    }
  }

  void dispatchBatch(std::vector<InferenceJob>&& batch) {
    std::string batch_id = "batch-" + std::to_string(TimestampMs());
    std::cout << "[Orchestrator] Dispatching " << batch.size() << " request(s) in " << batch_id << "\n";

    auto worker = worker_registry_.chooseWorker();
    if (!worker) {
      std::cerr << "[Orchestrator] No healthy workers available for " << batch_id << "\n";
      for (auto& item : batch) {
        InferenceResponse error_response;
        error_response.set_request_id(item.request.request_id());
        error_response.set_status("NO_WORKER_AVAILABLE");
        item.promise->set_value(std::move(error_response));
      }
      return;
    }

    BatchRequest request;
    request.set_batch_id(batch_id);
    std::unordered_map<std::string, std::shared_ptr<std::promise<InferenceResponse>>> pending;
    pending.reserve(batch.size());

    for (auto& item : batch) {
      request.add_requests()->CopyFrom(item.request);
      pending.emplace(item.request.request_id(), item.promise);
    }

    ClientContext context;
    BatchResponse response;
    grpc::Status status = worker->stub->ProcessBatch(&context, request, &response);
    if (!status.ok()) {
      std::cerr << "[Orchestrator] Worker " << worker->worker_id << " failed to process " << batch_id << ": " << status.error_message() << "\n";
      for (auto& kv : pending) {
        InferenceResponse error_response;
        error_response.set_request_id(kv.first);
        error_response.set_status("WORKER_FAILURE");
        kv.second->set_value(std::move(error_response));
      }
      return;
    }

    for (const auto& out : response.responses()) {
      auto it = pending.find(out.request_id());
      if (it != pending.end()) {
        it->second->set_value(out);
        pending.erase(it);
      }
    }

    for (auto& kv : pending) {
      InferenceResponse error_response;
      error_response.set_request_id(kv.first);
      error_response.set_status("MISSING_RESPONSE");
      kv.second->set_value(std::move(error_response));
    }

    std::cout << "[Orchestrator] Received " << response.responses_size() << " responses from " << response.worker_id() << " for " << batch_id << "\n";
  }

  void healthLoop() {
    const std::chrono::milliseconds heartbeat_timeout{7000};
    while (running_.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      worker_registry_.pruneExpired(heartbeat_timeout.count());
    }
  }

  std::string listen_address_;
  LockFreeMPSCQueue<InferenceJob> request_queue_;
  std::condition_variable batch_cv_;
  std::mutex batch_mutex_;
  std::atomic<bool> running_;
  WorkerRegistry worker_registry_;
};

int main(int argc, char** argv) {
  std::string address = "0.0.0.0:50051";
  if (argc > 1) {
    address = argv[1];
  }

  OrchestratorDaemon daemon(address);
  daemon.run();
  return 0;
}

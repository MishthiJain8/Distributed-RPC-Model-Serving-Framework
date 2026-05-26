#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "serving.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using grpc::Channel;

using serving::BatchRequest;
using serving::BatchResponse;
using serving::HeartbeatAck;
using serving::HeartbeatMessage;
using serving::InferenceRequest;
using serving::InferenceResponse;
using serving::WorkerHeartbeat;
using serving::WorkerNode;
using serving::WorkerRegistration;
using serving::WorkerStatus;

static int64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

struct WorkerState {
  std::atomic<int> active_tasks{0};
};

class WorkerNodeServiceImpl final : public WorkerNode::Service {
 public:
  WorkerNodeServiceImpl(std::string worker_id, std::shared_ptr<WorkerState> state)
      : worker_id_(std::move(worker_id)), state_(std::move(state)) {}

  Status ProcessBatch(ServerContext* context, const BatchRequest* request, BatchResponse* response) override {
    int active = ++state_->active_tasks;
    std::cout << "[Worker " << worker_id_ << "] Received " << request->requests_size() << " request(s) in " << request->batch_id() << " (active tasks=" << active << ")\n";
    response->set_worker_id(worker_id_);

    for (const auto& req : request->requests()) {
      InferenceResponse* out = response->add_responses();
      out->set_request_id(req.request_id());
      out->set_worker_id(worker_id_);
      float accumulator = 0.0f;
      for (float value : req.values()) {
        accumulator += value * 1.23f;
      }
      out->add_outputs(accumulator + 0.5f);
      out->set_status("COMPLETED");
      std::this_thread::sleep_for(std::chrono::milliseconds(40));
    }

    --state_->active_tasks;
    std::cout << "[Worker " << worker_id_ << "] Completed batch " << request->batch_id() << "\n";
    return Status::OK;
  }

 private:
  std::string worker_id_;
  std::shared_ptr<WorkerState> state_;
};

class HeartbeatClient {
 public:
  HeartbeatClient(std::shared_ptr<Channel> channel, const std::string& worker_id, const std::string& worker_address,
                  std::shared_ptr<WorkerState> state)
      : stub_(WorkerHeartbeat::NewStub(channel)), worker_id_(worker_id), worker_address_(worker_address), running_(true), state_(std::move(state)) {}

  void run() {
    ClientContext context;
    auto stream = stub_->Heartbeat(&context);

    HeartbeatMessage registration_msg;
    WorkerRegistration* reg = registration_msg.mutable_registration();
    reg->set_worker_id(worker_id_);
    reg->set_worker_address(worker_address_);
    if (!stream->Write(registration_msg)) {
      std::cerr << "[Worker " << worker_id_ << "] Failed to write registration heartbeat\n";
      return;
    }

    std::jthread ack_reader([this, &stream]() {
      HeartbeatAck ack;
      while (stream->Read(&ack)) {
        std::cout << "[Worker " << worker_id_ << "] Heartbeat ack: " << ack.message() << "\n";
      }
    });

    while (running_.load(std::memory_order_relaxed)) {
      HeartbeatMessage heartbeat_msg;
      WorkerStatus* status = heartbeat_msg.mutable_status();
      status->set_worker_id(worker_id_);
      status->set_active_tasks(state_->active_tasks.load(std::memory_order_relaxed));
      status->set_timestamp_ms(TimestampMs());
      if (!stream->Write(heartbeat_msg)) {
        std::cerr << "[Worker " << worker_id_ << "] Failed to write heartbeat status\n";
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    stream->WritesDone();
    grpc::Status status = stream->Finish();
    if (!status.ok()) {
      std::cerr << "[Worker " << worker_id_ << "] Heartbeat stream finished with error: " << status.error_message() << "\n";
    }
  }

 private:
  std::shared_ptr<WorkerHeartbeat::Stub> stub_;
  std::string worker_id_;
  std::string worker_address_;
  std::atomic<bool> running_;
  std::shared_ptr<WorkerState> state_;
};

int main(int argc, char** argv) {
  const std::string orchestrator_address = (argc > 1 ? argv[1] : "localhost:50051");
  const std::string worker_address = (argc > 2 ? argv[2] : "0.0.0.0:50052");
  const std::string worker_id = (argc > 3 ? argv[3] : "worker-1");

  std::cout << "[Worker " << worker_id << "] Starting, connecting to orchestrator at " << orchestrator_address << " and listening on " << worker_address << "\n";

  auto state = std::make_shared<WorkerState>();
  WorkerNodeServiceImpl service(worker_id, state);

  ServerBuilder builder;
  builder.AddListeningPort(worker_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "[Worker " << worker_id << "] Failed to start worker server\n";
    return 1;
  }

  std::thread heartbeat_thread([&]() {
    HeartbeatClient client(grpc::CreateChannel(orchestrator_address, grpc::InsecureChannelCredentials()), worker_id, worker_address, state);
    client.run();
  });

  server->Wait();
  if (heartbeat_thread.joinable()) {
    heartbeat_thread.join();
  }

  return 0;
}

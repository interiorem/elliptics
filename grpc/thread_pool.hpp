#pragma once

#include <thread>
#include <vector>

#include <grpcpp/completion_queue.h>

namespace ioremap { namespace grpc {

class completion_thread_pool_t {
public:
	completion_thread_pool_t(::grpc::CompletionQueue &completion_queue, std::size_t size);

	~completion_thread_pool_t();

private:
	::grpc::CompletionQueue &completion_queue_;
	std::vector<std::thread> threads_;
};

}} // namespace ioremap::grpc

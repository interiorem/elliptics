#include "thread_pool.hpp"

#include "job.hpp"

namespace ioremap { namespace grpc {

completion_thread_pool_t::completion_thread_pool_t(::grpc::CompletionQueue &completion_queue, std::size_t size)
: completion_queue_(completion_queue) {
	auto loop = [this] {
		void *tag;
		bool ok;
		while (completion_queue_.Next(&tag, &ok)) {
			static_cast<job_t*>(tag)->proceed(ok);
		}
	};
	threads_.reserve(size);
	for (std::size_t i = 0; i < size; ++i) {
		threads_.emplace_back(loop);
	}
}

completion_thread_pool_t::~completion_thread_pool_t() {
	completion_queue_.Shutdown();
	for (auto &t : threads_) {
		t.join();
	}
}

}} // namespace ioremap::grpc

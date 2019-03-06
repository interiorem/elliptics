#include "grpc.h"

#include <blackhole/attribute.hpp>

#include "library/elliptics.h"
#include "library/logger.hpp"
#include "server.hpp"
#include "thread_pool.hpp"

struct dnet_grpc_io {
	std::shared_ptr<::grpc::CompletionQueue> completion_queue;
	std::unique_ptr<ioremap::grpc::completion_thread_pool_t> thread_pool;
	std::unique_ptr<ioremap::grpc::server_t> server;
};

int dnet_grpc_io_start(struct dnet_node *node, const char *address, unsigned thread_num) {
	using namespace ioremap::grpc;

	DNET_LOG_INFO(node, "GRPC: Start gRPC IO");
	try {
		auto grpc = std::make_unique<dnet_grpc_io>();

		if (address) {
			grpc->server = std::make_unique<server_t>(*node, address);
			grpc->completion_queue = grpc->server->completion_queue();
		} else {
			// TODO: Use it in client
			grpc->completion_queue = std::make_shared<grpc::CompletionQueue>();
		}
		grpc->thread_pool = std::make_unique<completion_thread_pool_t>(*grpc->completion_queue, thread_num);
		if (grpc->server) {
			grpc->server->start();
		}

		node->grpc = grpc.release();
	} catch (const std::bad_alloc &) {
		DNET_LOG_ERROR(node, "GRPC: Start gRPC IO failed: no memory");
		return -ENOMEM;
	} catch (const std::exception &e) {
		DNET_LOG_ERROR(node, "GRPC: Start gRPC IO failed: {}", e.what());
		return -EIO;
	}
	DNET_LOG_INFO(node, "GRPC: Start gRPC IO successfully finished");
	return 0;
}

void dnet_grpc_io_stop(struct dnet_node *node) {
	DNET_LOG_INFO(node, "GRPC: Stop gRPC IO");
	delete std::exchange(node->grpc, nullptr);
	DNET_LOG_INFO(node, "GRPC: Stop gRPC IO successfully finished");
}

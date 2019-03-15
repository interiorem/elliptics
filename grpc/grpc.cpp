#include "grpc.h"

#include <blackhole/attribute.hpp>

#include "config.h"
#include "library/elliptics.h"
#include "library/logger.hpp"
#include "server.hpp"
#include "thread_pool.hpp"

namespace {

int safe_grpc_io_start(dnet_node &node, std::function<void(dnet_grpc_io &)> &&custom) {
	DNET_LOG_INFO(node, "GRPC: Start gRPC IO");
	try {
		auto grpc = std::make_unique<dnet_grpc_io>();

		custom(*grpc);

		node.io->grpc = grpc.release();
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

}

struct dnet_grpc_io {
	std::shared_ptr<::grpc::CompletionQueue> completion_queue;
	std::unique_ptr<ioremap::grpc::completion_thread_pool_t> thread_pool;
	std::unique_ptr<ioremap::grpc::server_t> server;
};

int dnet_grpc_io_client_start(struct dnet_node *node, struct dnet_grpc_client_config *config) {
	return safe_grpc_io_start(*node, [&](dnet_grpc_io &grpc) {
		grpc.completion_queue = std::make_shared<grpc::CompletionQueue>();
		grpc.thread_pool = std::make_unique<ioremap::grpc::completion_thread_pool_t>(
		        *grpc.completion_queue, config->thread_num);
	});
}

int dnet_grpc_io_server_start(struct dnet_node *node, struct dnet_grpc_server_config *config) {
	return safe_grpc_io_start(*node, [&](dnet_grpc_io &grpc) {
		grpc.server = std::make_unique<ioremap::grpc::server_t>(*node, config->address);
		grpc.completion_queue = grpc.server->completion_queue();
		grpc.thread_pool = std::make_unique<ioremap::grpc::completion_thread_pool_t>(
		        *grpc.completion_queue, config->client.thread_num);
		grpc.server->start();
	});
}

void dnet_grpc_io_stop(struct dnet_node *node) {
	DNET_LOG_INFO(node, "GRPC: Stop gRPC IO");
	delete std::exchange(node->io->grpc, nullptr);
	DNET_LOG_INFO(node, "GRPC: Stop gRPC IO successfully finished");
}

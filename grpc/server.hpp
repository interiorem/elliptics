#pragma once

#include <grpcpp/server.h>

#include "elliptics.grpc.fb.h"

namespace ioremap { namespace grpc {

class server_t {
public:
	server_t(dnet_node &node, const std::string &address);

	std::shared_ptr<::grpc::ServerCompletionQueue> completion_queue() const;

	void start();

private:
	dnet_node &node_;
	std::shared_ptr<::grpc::ServerCompletionQueue> completion_queue_;
	fb_grpc_dnet::Elliptics::AsyncService async_service_;
	std::unique_ptr<::grpc::Server> server_;
};

}} // namespace ioremap::grpc

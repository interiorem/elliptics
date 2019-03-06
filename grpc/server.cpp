#include "server.hpp"

#include <blackhole/attribute.hpp>
#include <grpcpp/server_builder.h>

#include "library/logger.hpp"
#include "read_job.hpp"
#include "write_job.hpp"

namespace ioremap { namespace grpc {

server_t::server_t(dnet_node &node, const std::string &address)
: node_(node) {
	::grpc::ServerBuilder builder;
	builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
	builder.RegisterService(&async_service_);
	completion_queue_ = builder.AddCompletionQueue();
	server_ = builder.BuildAndStart();
	if (!server_) {
		throw std::runtime_error("Failed to start gRPC server. See log.");
	}
	DNET_LOG_INFO(node, "GRPC: server listening on {}", address);
}

std::shared_ptr<::grpc::ServerCompletionQueue> server_t::completion_queue() const {
	return completion_queue_;
}

void server_t::start() {
	new read_job_t(node_, *completion_queue_, async_service_);
	new write_job_t(node_, *completion_queue_, async_service_);
}

}} // namespace ioremap::grpc

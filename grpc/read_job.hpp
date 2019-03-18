#pragma once

#include "elliptics.grpc.fb.h"
#include "job.hpp"
#include "library/n2_protocol.hpp"

namespace ioremap { namespace grpc {

class read_job_t : public job_t {
/*
 * This class is for handling of read requests. One object of the read_job_t class stores
 * a protocol state for one RPC request. The protocol states are switched by calling
 * the proceed(bool) method from outside.
 *
 * States:
 *      REQUEST_WAITING - Ready for request.
 *      RESPONSE_PARTIAL_COMPLETE - Request is received, accepted and part of response ready to write.
 *      RESPONSE_COMPLETE - Response is totally complete and last part ready to write.
 */

public:
	using request_t = elliptics::n2::read_request;
	using response_t = elliptics::n2::read_response;
	using rpc_request_t = flatbuffers::grpc::Message<fb_grpc_dnet::ReadRequest>;
	using rpc_response_t = flatbuffers::grpc::Message<fb_grpc_dnet::ReadResponse>;

	read_job_t(dnet_node &node, ::grpc::ServerCompletionQueue &cq, fb_grpc_dnet::Elliptics::AsyncService &service);

	void proceed(bool ok) override;

private:
	enum class state_t {
		REQUEST_WAITING,
		RESPONSE_PARTIAL_COMPLETE,
		RESPONSE_COMPLETE,
	};

	void push_request();
	void send_response(std::unique_ptr<response_t> response);
	void send_next(bool first);

	::grpc::ServerContext ctx_;
	::grpc::ServerAsyncWriter<rpc_response_t> async_writer_;

	dnet_node &node_;
	::grpc::ServerCompletionQueue &completion_queue_;
	fb_grpc_dnet::Elliptics::AsyncService &async_service_;

	state_t state_ = state_t::REQUEST_WAITING;

	rpc_request_t rpc_request_;
	std::unique_ptr<response_t> response_;
	size_t response_json_offset_ = 0;
	size_t response_data_offset_ = 0;
};

}} // namespace ioremap::grpc

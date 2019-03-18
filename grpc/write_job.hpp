#pragma once

#include "elliptics.grpc.fb.h"
#include "job.hpp"
#include "library/n2_protocol.hpp"

namespace ioremap { namespace grpc {

class write_job_t : public job_t {
/*
 * This class is for handling of write requests. One object of the write_job_t class stores
 * a protocol state for one RPC request. The protocol states are switched by calling
 * the proceed(bool) method from outside.
 *
 * States:
 *      REQUEST_WAITING_FIRST - Ready for first part of request.
 *      REQUEST_WAITING_NEXT - First part is received, ready for other parts of request.
 *      RESPONSE_COMPLETE - Response is totally complete and ready to write.
 */

public:
	using request_t = elliptics::n2::write_request;
	using response_t = elliptics::n2::lookup_response;
	using rpc_request_t = flatbuffers::grpc::Message<fb_grpc_dnet::WriteRequest>;
	using rpc_response_t = flatbuffers::grpc::Message<fb_grpc_dnet::LookupResponse>;

	write_job_t(dnet_node &node, ::grpc::ServerCompletionQueue &cq, fb_grpc_dnet::Elliptics::AsyncService &service);

private:
	enum class state_t {
		REQUEST_WAITING_FIRST,
		REQUEST_WAITING_NEXT,
		RESPONSE_COMPLETE,
	};

	void proceed(bool more) override;

	void read_next(bool first, bool more);
	void push_request();
	void send_response(std::unique_ptr<response_t> response);

	::grpc::ServerContext ctx_;
	::grpc::ServerAsyncReader<rpc_response_t, rpc_request_t> async_reader_;

	dnet_node &node_;
	::grpc::ServerCompletionQueue &completion_queue_;
	fb_grpc_dnet::Elliptics::AsyncService &async_service_;

	state_t state_ = state_t::REQUEST_WAITING_FIRST;

	std::unique_ptr<request_t> request_;
	size_t request_json_offset_ = 0;
	size_t request_data_offset_ = 0;
};

}} // namespace ioremap::grpc

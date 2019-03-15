#pragma once

#include "elliptics.grpc.fb.h"
#include "job.hpp"
#include "library/n2_protocol.hpp"

namespace ioremap { namespace grpc {

class write_job_t : public job_t {
public:
	using request_t = elliptics::n2::write_request;
	using response_t = elliptics::n2::lookup_response;
	using rpc_request_t = flatbuffers::grpc::Message<fb_grpc_dnet::WriteRequest>;
	using rpc_response_t = flatbuffers::grpc::Message<fb_grpc_dnet::LookupResponse>;

	write_job_t(dnet_node &node, ::grpc::ServerCompletionQueue &cq, fb_grpc_dnet::Elliptics::AsyncService &service);

private:
	class responder_t;
	friend responder_t;

	enum class state {
		CREATE,
		REQUEST_RECEIVED_FIRST,
		REQUEST_RECEIVED,
		FINISH,
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

	state state_ = state::CREATE;

	std::unique_ptr<request_t> request_;
	size_t request_json_offset_ = 0;
	size_t request_data_offset_ = 0;
};

}} // namespace ioremap::grpc

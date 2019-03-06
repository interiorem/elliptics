#pragma once

#include "elliptics.grpc.fb.h"
#include "job.hpp"
#include "library/n2_protocol.hpp"

namespace ioremap { namespace grpc {

class read_job_t : public job_t {
public:
	using request_t = elliptics::n2::read_request;
	using response_t = elliptics::n2::read_response;
	using rpc_request_t = flatbuffers::grpc::Message<fb_grpc_dnet::ReadRequest>;
	using rpc_response_t = flatbuffers::grpc::Message<fb_grpc_dnet::ReadResponse>;

	read_job_t(dnet_node &node, ::grpc::ServerCompletionQueue &cq, fb_grpc_dnet::Elliptics::AsyncService &service);

	void proceed(bool ok) override;

private:
	class responder_t;
	friend responder_t;

	enum class state {
		CREATE,
		REQUEST_RECEIVED,
		RESPONSE_PARTIAL_COMPLETE,
		FINISH,
	};

	void push_request();
	void send_response(std::unique_ptr<response_t> response);
	void send_next(bool first);

private:
	::grpc::ServerContext ctx_;
	::grpc::ServerAsyncWriter<rpc_response_t> async_writer_;

	dnet_node &node_;
	::grpc::ServerCompletionQueue &completion_queue_;
	fb_grpc_dnet::Elliptics::AsyncService &async_service_;

	state state_ = state::CREATE;

	rpc_request_t rpc_request_;
	std::unique_ptr<response_t> response_;
	size_t response_json_offset_ = 0;
	size_t response_data_offset_ = 0;
};

}} // namespace ioremap::grpc

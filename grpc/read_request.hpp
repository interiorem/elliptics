#pragma once

#include "requests_common.hpp"

namespace ioremap { namespace elliptics { namespace grpc_dnet {

class read_request_manager : public request_manager_base {
public:
	using request_t = n2::read_request;
	using response_t = n2::read_response;
	using handler_t = std::function<void (std::unique_ptr<request_t>)>;

	read_request_manager(service_provider_info &service_provider_info_in, handler_t request_processor_in);

	void send_response(std::unique_ptr<response_t> response);

private:
	enum class on_complete_op {
		HANDLE_REQUEST,
		HANDLE_SEND_PARTIAL_COMPLETE,
		HANDLE_SEND_ALL_COMPLETE,
		DELETE,
	};

	using rpc_request_t = flatbuffers::grpc::Message<fb_grpc_dnet::ReadRequest>;
	using rpc_response_t = flatbuffers::grpc::Message<fb_grpc_dnet::ReadResponse>;

	virtual void process_completion(bool more) override;

	void handle_request();

	on_complete_op send_next();
	void send_finish_marker();

	static void deserialize(const rpc_request_t &rpc_request, request_t &request);

	rpc_response_t serialize_next(const response_t &response, bool &complete);

	static flatbuffers::Offset<flatbuffers::Vector<uint8_t>>
	put_data_part(flatbuffers::grpc::MessageBuilder &builder,
	              const data_pointer &total_data,
	              size_t &current_part_offset);

	on_complete_op on_complete_op_;

	rpc_request_t rpc_request_;
	grpc::ServerAsyncWriter<rpc_response_t> rpc_call_io_;

	std::unique_ptr<response_t> response_;
	data_pointer data_;
	flatbuffers::grpc::MessageBuilder builder_;
	size_t response_json_offset_;
	size_t response_data_offset_;

	handler_t handler_;
};

}}} // namespace ioremap::elliptics::grpc_dnet

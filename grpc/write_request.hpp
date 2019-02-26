#pragma once

#include "requests_common.hpp"

namespace ioremap { namespace elliptics { namespace grpc_dnet {

class write_request_manager : public request_manager_base {
public:
	using request_t = n2::write_request;
	using response_t = n2::lookup_response;
	using handler_t = std::function<void (std::unique_ptr<request_t>)>;

	write_request_manager(service_provider_info &service_provider_info_in, handler_t request_processor_in);

	void send_response(std::unique_ptr<response_t> response);

private:
	enum class on_complete_op {
		READ_REQUEST_FIRST,
		READ_REQUEST,
		DELETE,
	};

	using rpc_request_t = flatbuffers::grpc::Message<fb_grpc_dnet::WriteRequest>;
	using rpc_response_t = flatbuffers::grpc::Message<fb_grpc_dnet::LookupResponse>;

	virtual void process_completion(bool more) override;

	void read_next();
	void handle_final();

	void deserialize_next(const rpc_request_t &rpc_request, request_t &request);

	static void append_data(data_pointer &total_data, size_t &current_part_offset,
	                        const flatbuffers::Vector<uint8_t> *current_part);

	static rpc_response_t serialize(const response_t &response);

	on_complete_op on_complete_op_;

	grpc::ServerAsyncReader<rpc_response_t, rpc_request_t> rpc_call_io_;

	std::unique_ptr<request_t> request_;
	size_t request_json_offset_;
	size_t request_data_offset_;

	handler_t handler_;
};

}}} // namespace ioremap::elliptics::grpc_dnet

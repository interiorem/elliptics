#include "write_request.hpp"

namespace ioremap { namespace elliptics { namespace grpc_dnet {

write_request_manager::write_request_manager(service_provider_info &service_provider_info_in, handler_t handler_in)
: request_manager_base(service_provider_info_in)
, rpc_call_io_(&ctx_)
, request_(new request_t)
, request_json_offset_(0)
, request_data_offset_(0)
, handler_(std::move(handler_in))
{
	on_complete_op_ = on_complete_op::READ_REQUEST_FIRST;
	provider_.service->RequestWrite(&ctx_, &rpc_call_io_, provider_.cq, provider_.cq, this);
}

void write_request_manager::send_response(std::unique_ptr<response_t> response) {
	on_complete_op_ = on_complete_op::DELETE;
	rpc_call_io_.Finish(serialize(*response), grpc::Status::OK, this);
}

void write_request_manager::process_completion(bool more) {
	if (!more) {
		// TODO Fix it. write_request_manager does not work
		// On gRPC shutdown CompletionQueue calls proceed(more) with flag more=false
		// and the same for finish of successfully write.
		on_complete_op_ = on_complete_op::DELETE;
	}
	switch (on_complete_op_) {
	case on_complete_op::READ_REQUEST_FIRST:
		on_complete_op_ = on_complete_op::READ_REQUEST;
		new write_request_manager(provider_, handler_);
		// falls through

	case on_complete_op::READ_REQUEST:
		if (more) {
			read_next();
		} else {
			handle_final();
		}
		break;

	case on_complete_op::DELETE:
		delete this;
		break;
	}
}

void write_request_manager::read_next() { // TODO: maybe do it async
	rpc_request_t rpc_request;
	rpc_call_io_.Read(&rpc_request, this);
	deserialize_next(rpc_request, *request_);
}

void write_request_manager::handle_final() { // TODO: maybe do it async
	if (request_json_offset_ != request_->json.size() || request_data_offset_ != request_->data.size()) {
		throw std::invalid_argument("Incomplete buffer");
	}

	time_point_to_dnet_time(ctx_.deadline(), request_->deadline);

	handler_(std::move(request_));
}

void write_request_manager::deserialize_next(const rpc_request_t &rpc_request, request_t &request) {
	auto fb_request = rpc_request.GetRoot();
	auto fb_request_header = fb_request->header();
	if (fb_request_header) {
		deserialize_cmd(fb_request_header->cmd(), request.cmd);
		request.ioflags = fb_request_header->ioflags();
		request.user_flags = fb_request_header->user_flags();
		nanosec_to_dnet_time(fb_request_header->data_timestamp(), request.data_timestamp);
		request.json_size = fb_request_header->json_size();
		request.json_capacity = fb_request_header->json_capacity();
		nanosec_to_dnet_time(fb_request_header->json_timestamp(), request.json_timestamp);
		request.data_offset = fb_request_header->data_offset();
		request.data_size = fb_request_header->data_size();
		request.data_capacity = fb_request_header->data_capacity();
		request.data_commit_size = fb_request_header->data_commit_size();
		request.cache_lifetime = fb_request_header->cache_lifetime();

		request.json = data_pointer::allocate(request.json_size);
		request.data = data_pointer::allocate(request.data_size);
	}

	append_data(request.json, request_json_offset_, fb_request->json());
	append_data(request.data, request_data_offset_, fb_request->data());
}

void write_request_manager::append_data(data_pointer &total_data, size_t &current_part_offset,
                                        const flatbuffers::Vector<uint8_t> *current_part) {
	if (!current_part) {
		return;
	}

	auto current_part_size = current_part->size();
	size_t next_part_offset = current_part_offset + current_part_size;
	if (next_part_offset > total_data.size()) {
		throw std::invalid_argument("Buffer overflow");
	}

	memcpy(total_data.skip(current_part_offset).data(), current_part->data(), current_part_size);
	current_part_offset = next_part_offset;
}

write_request_manager::rpc_response_t write_request_manager::serialize(const response_t &response) {
	flatbuffers::grpc::MessageBuilder builder;

	auto fb_response = fb_grpc_dnet::CreateLookupResponse(
		builder,
		serialize_cmd(builder, response.cmd),
		response.record_flags,
		response.user_flags,
		builder.CreateString(response.path),
		dnet_time_to_nanosec(response.json_timestamp),
		response.json_offset,
		response.json_size,
		response.json_capacity,
		builder.CreateVector(response.json_checksum),
		dnet_time_to_nanosec(response.data_timestamp),
		response.data_offset,
		response.data_size,
		builder.CreateVector(response.data_checksum)
	);

	builder.Finish(fb_response);

	return builder.ReleaseMessage<fb_grpc_dnet::LookupResponse>();
}

}}} // namespace ioremap::elliptics::grpc_dnet

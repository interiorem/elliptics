#include "read_request.hpp"

namespace ioremap { namespace elliptics { namespace grpc_dnet {

read_request_manager::read_request_manager(service_provider_info &provider_in, handler_t handler_in)
: request_manager_base(provider_in)
, rpc_call_io_(&ctx_)
, response_json_offset_(0)
, response_data_offset_(0)
, handler_(std::move(handler_in))
{
	on_complete_op_ = on_complete_op::HANDLE_REQUEST;
	provider_.service->RequestRead(&ctx_, &rpc_request_, &rpc_call_io_, provider_.cq, provider_.cq, this);
}

void read_request_manager::send_response(std::unique_ptr<response_t> response) {
	response_ = std::move(response);
	on_complete_op_ = send_next();
}

void read_request_manager::process_completion(bool ok) {
        if (!ok) {
                //TODO log
                on_complete_op_ = on_complete_op::DELETE;
        }
	switch (on_complete_op_) {
	case on_complete_op::HANDLE_REQUEST:
		new read_request_manager(provider_, handler_);
		handle_request();
		break;

	case on_complete_op::HANDLE_SEND_PARTIAL_COMPLETE:
		on_complete_op_ = send_next();
		break;

	case on_complete_op::HANDLE_SEND_ALL_COMPLETE:
		send_finish_marker();
		on_complete_op_ = on_complete_op::DELETE;
		break;

	case on_complete_op::DELETE:
		delete this;
		break;
	}
}

void read_request_manager::handle_request() {
	std::unique_ptr<request_t> request(new request_t);
	deserialize(rpc_request_, *request);
	time_point_to_dnet_time(ctx_.deadline(), request->deadline);

	handler_(std::move(request));
}

read_request_manager::on_complete_op read_request_manager::send_next() {
	bool complete;
	rpc_call_io_.Write(serialize_next(*response_, complete), this);

	return complete ? on_complete_op::HANDLE_SEND_ALL_COMPLETE : on_complete_op::HANDLE_SEND_PARTIAL_COMPLETE;
}

void read_request_manager::send_finish_marker() {
        rpc_call_io_.Finish(grpc::Status::OK, this);
}

void read_request_manager::deserialize(const rpc_request_t &rpc_request, request_t &request) {
	auto fb_request = rpc_request.GetRoot();

	deserialize_cmd(fb_request->cmd(), request.cmd);
	request.ioflags = fb_request->ioflags();
	request.read_flags = fb_request->read_flags();
	request.data_offset = fb_request->data_offset();
	request.data_size = fb_request->data_size();
}

read_request_manager::rpc_response_t read_request_manager::serialize_next(const response_t &response, bool &complete) {
	//Only first message has header
	flatbuffers::Offset<fb_grpc_dnet::ReadResponseHeader> fb_response_header = 0;
	if (!response_json_offset_ && !response_data_offset_) {
		fb_response_header = fb_grpc_dnet::CreateReadResponseHeader(
			builder_,
			serialize_cmd(builder_, response.cmd),
			response.record_flags,
			response.user_flags,
			dnet_time_to_nanosec(response.json_timestamp),
			response.json_size,
			response.json_capacity,
			response.read_json_size,
			dnet_time_to_nanosec(response.data_timestamp),
			response.data_size,
			response.read_data_offset,
			response.read_data_size
		);

		if (response.data.where() == n2::data_place::IN_MEMORY) {
			data_ = response.data.in_memory;

		} else /*data_place::IN_FILE*/ {
			// TODO: implement, see function dnet_io_req_copy as example
			throw std::invalid_argument("read_request_manager::serialize_next doesn\'t work with fd yet");
		}
	}

	auto fb_response = fb_grpc_dnet::CreateReadResponse(
		builder_,
		fb_response_header,
		put_data_part(builder_, response.json, response_json_offset_),
		put_data_part(builder_, data_, response_data_offset_)
	);

	complete = (response_json_offset_ == response.json.size() && response_data_offset_ == data_.size());

	builder_.Finish(fb_response);

	// this also resets the builder
	return builder_.ReleaseMessage<fb_grpc_dnet::ReadResponse>();
}

flatbuffers::Offset<flatbuffers::Vector<uint8_t>>
read_request_manager::put_data_part(flatbuffers::grpc::MessageBuilder &builder,
                                    const data_pointer &total_data,
                                    size_t &current_part_offset) {
	if (current_part_offset == total_data.size()) {
		// Whole buffer already serialized
		return 0;
	}

	if (builder.GetSize() >= MAX_MESSAGE_SIZE) {
		// No place to serialize some
		return 0;
	}

	size_t max_part_size = MAX_MESSAGE_SIZE - builder.GetSize();
	size_t size_left = total_data.size() - current_part_offset;
	size_t part_size = std::min(size_left, max_part_size);

	auto part_ptr = static_cast<const uint8_t *>(total_data.skip(current_part_offset).data());
	auto res = builder.CreateVector(part_ptr, part_size);
	current_part_offset =+ part_size;
	return res;
}

}}} // namespace ioremap::elliptics::grpc_dnet

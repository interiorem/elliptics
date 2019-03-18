#include "read_job.hpp"

#include "library/request_queue.h"
#include "serialization.hpp"

namespace ioremap { namespace grpc { namespace {

void deserialize(const read_job_t::rpc_request_t &rpc_request, read_job_t::request_t &request) {
	auto fb_request = rpc_request.GetRoot();

	deserialize_cmd(fb_request->cmd(), request.cmd);
	request.cmd.cmd = dnet_commands::DNET_CMD_READ_NEW;
	request.ioflags = fb_request->ioflags();
	request.read_flags = fb_request->read_flags();
	request.data_offset = fb_request->data_offset();
	request.data_size = fb_request->data_size();
}

flatbuffers::Offset<fb_grpc_dnet::ReadResponseHeader> serialize_header(flatbuffers::grpc::MessageBuilder &builder,
	const read_job_t::response_t &response) {

	auto json_timestamp = to_rpc_time(response.json_timestamp);
	auto data_timestamp = to_rpc_time(response.data_timestamp);

	return fb_grpc_dnet::CreateReadResponseHeader(
		builder,
		serialize_cmd(builder, response.cmd),
		response.record_flags,
		response.user_flags,
		&json_timestamp,
		response.json_size,
		response.json_capacity,
		response.read_json_size,
		&data_timestamp,
		response.data_size,
		response.read_data_offset,
		response.read_data_size
	);
}

flatbuffers::Offset<flatbuffers::Vector<uint8_t>> put_data_part(flatbuffers::grpc::MessageBuilder &builder,
	const elliptics::data_pointer &data, size_t &offset) {

	if (offset == data.size() || builder.GetSize() >= GRPC_MAX_MESSAGE_SIZE) {
		return 0;
	}

	size_t max_part_size = GRPC_MAX_MESSAGE_SIZE - builder.GetSize();
	size_t size_left = data.size() - offset;
	size_t part_size = std::min(size_left, max_part_size);

	auto part_ptr = static_cast<const uint8_t *>(data.skip(offset).data());
	auto res = builder.CreateVector(part_ptr, part_size);
	offset += part_size;
	return res;
}

read_job_t::rpc_response_t serialize_part(const read_job_t::response_t &response, bool first,
	std::size_t &json_offset, std::size_t &data_offset) {

	flatbuffers::grpc::MessageBuilder builder;

	auto header = first
	              ? serialize_header(builder, response)
	              : flatbuffers::Offset<fb_grpc_dnet::ReadResponseHeader>();

	builder.Finish(fb_grpc_dnet::CreateReadResponse(
		builder,
		header,
		put_data_part(builder, response.json, json_offset),
		put_data_part(builder, response.data.in_memory, data_offset)
	));
	return builder.ReleaseMessage<fb_grpc_dnet::ReadResponse>();
}

// TODO: Move to common place?
class responder_t {
public:
	using on_complete_t = std::function<void (std::unique_ptr<read_job_t::response_t>)>;

	explicit responder_t(on_complete_t &&on_complete)
	: on_complete_(std::move(on_complete)) {
	}

	void reply(std::unique_ptr<read_job_t::response_t> response) {
		on_complete_(std::move(response));
	}

	void reply_error(int /*code*/, std::string&& /*message*/) {
		// TODO(artsel)
	}

private:
	const on_complete_t on_complete_;
};

} // namespace

read_job_t::read_job_t(dnet_node &node, ::grpc::ServerCompletionQueue &completion_queue,
	fb_grpc_dnet::Elliptics::AsyncService &service)
: async_writer_(&ctx_)
, node_(node)
, completion_queue_(completion_queue)
, async_service_(service) {
	async_service_.RequestRead(&ctx_, &rpc_request_, &async_writer_, &completion_queue_, &completion_queue_, this);
}

void read_job_t::proceed(bool ok) {
	// TODO: catch exceptions here
	if (!ok) {
	    // TODO log
	    state_ = state_t::RESPONSE_COMPLETE;
	}
	switch (state_) {
	case state_t::REQUEST_WAITING:
		new read_job_t(node_, completion_queue_, async_service_);
		push_request();
		break;
	case state_t::RESPONSE_PARTIAL_COMPLETE:
		send_next(false /*first*/);
		break;
	case state_t::RESPONSE_COMPLETE:
		delete this;
		break;
	}
}

void read_job_t::push_request() {
	// TODO: Use constructor or clear memory for request
	auto request = new request_t;
	deserialize(rpc_request_, *request);
	request->deadline = to_dnet_time(ctx_.deadline());

	// TODO: add to req
	// #include <functional>
	// auto responder = new responder_t(std::bind(&read_job_t::send_response, this, std::placeholders::_1));

	auto req = new dnet_io_req;
	memset(req, 0, sizeof(dnet_io_req));
	req->st = node_.st;
	req->header = static_cast<void*>(&request->cmd);
	req->n2_msg = request;

	dnet_schedule_io(&node_, req);
}

void read_job_t::send_response(std::unique_ptr<response_t> response) {
	response_ = std::move(response);
	send_next(true /*first*/);
}

void read_job_t::send_next(bool first) {
	if (response_->data.where() == elliptics::n2::data_place::IN_FILE) {
		// TODO: implement, see function dnet_io_req_copy as example
		throw std::invalid_argument("read_request_manager::serialize_next doesn't work with fd yet");
	}

	auto response = serialize_part(*response_, first, response_json_offset_, response_data_offset_);

	if (response_data_offset_ == response_->json_size && response_data_offset_ == response_->data_size) {
		state_ = state_t::RESPONSE_COMPLETE;
		async_writer_.WriteAndFinish(response, ::grpc::WriteOptions(), ::grpc::Status::OK, this);
	} else {
		state_ = state_t::RESPONSE_PARTIAL_COMPLETE;
		async_writer_.Write(response, this);
	}
}

}} // namespace ioremap::grpc

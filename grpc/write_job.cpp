#include "write_job.hpp"

#include "library/request_queue.h"
#include "serialization.hpp"

namespace ioremap { namespace grpc { namespace {

void put_data_part(elliptics::data_pointer &data, size_t &offset, const flatbuffers::Vector<uint8_t> *part) {
	if (!part) {
		return;
	}

	auto part_size = part->size();
	size_t new_offset = part_size + offset;
	if (new_offset > data.size()) {
		// TODO: no throw
		throw std::invalid_argument("Buffer overflow");
	}

	memcpy(data.skip(offset).data(), part->data(), part_size);
	offset = new_offset;
}

void deserialize_header(const fb_grpc_dnet::WriteRequestHeader *fb_header, write_job_t::request_t &request) {
	deserialize_cmd(fb_header->cmd(), request.cmd);
	request.cmd.cmd = dnet_commands::DNET_CMD_WRITE_NEW;
	request.ioflags = fb_header->ioflags();
	request.user_flags = fb_header->user_flags();
	request.json_timestamp = to_dnet_time(fb_header->json_timestamp());
	request.json_size = fb_header->json_size();
	request.json_capacity = fb_header->json_capacity();
	request.data_timestamp = to_dnet_time(fb_header->data_timestamp());
	request.data_offset = fb_header->data_offset();
	request.data_size = fb_header->data_size();
	request.data_capacity = fb_header->data_capacity();
	request.data_commit_size = fb_header->data_commit_size();
	request.cache_lifetime = fb_header->cache_lifetime();

	request.json = elliptics::data_pointer::allocate(request.json_size);
	request.data = elliptics::data_pointer::allocate(request.data_size);
}

void deserialize_part(const write_job_t::rpc_request_t &rpc_request, bool first, write_job_t::request_t &request,
	std::size_t &json_offset, std::size_t &data_offset) {

	auto fb_request = rpc_request.GetRoot();
	auto fb_header = fb_request->header();
	if (first && fb_header) {
		deserialize_header(fb_header, request);
	}
	put_data_part(request.json, json_offset, fb_request->json());
	put_data_part(request.data, data_offset, fb_request->data());
}

write_job_t::rpc_response_t serialize(const write_job_t::response_t &response) {
	flatbuffers::grpc::MessageBuilder builder;

	auto json_timestamp = to_rpc_time(response.json_timestamp);
	auto data_timestamp = to_rpc_time(response.data_timestamp);

	auto fb_response = fb_grpc_dnet::CreateLookupResponse(
		builder,
		serialize_cmd(builder, response.cmd),
		response.record_flags,
		response.user_flags,
		builder.CreateString(response.path),
		&json_timestamp,
		response.json_offset,
		response.json_size,
		response.json_capacity,
		builder.CreateVector(response.json_checksum),
		&data_timestamp,
		response.data_offset,
		response.data_size,
		builder.CreateVector(response.data_checksum)
	);

	builder.Finish(fb_response);

	return builder.ReleaseMessage<fb_grpc_dnet::LookupResponse>();
}

} // namespace

write_job_t::write_job_t(dnet_node &node, ::grpc::ServerCompletionQueue &completion_queue,
	fb_grpc_dnet::Elliptics::AsyncService &service)
: async_reader_(&ctx_)
, node_(node)
, completion_queue_(completion_queue)
, async_service_(service) {
	async_service_.RequestWrite(&ctx_, &async_reader_, &completion_queue_, &completion_queue_, this);
}

void write_job_t::proceed(bool more) {
	// TODO: catch exceptions here
	switch (state_) {
	case state_t::REQUEST_WAITING_FIRST:
		state_ = state_t::REQUEST_WAITING_NEXT;
		new write_job_t(node_, completion_queue_, async_service_);
		read_next(true /*first*/, more);
		break;
	case state_t::REQUEST_WAITING_NEXT:
		read_next(false /*first*/, more);
		break;
	case state_t::RESPONSE_COMPLETE:
		delete this;
		break;
	}
}

void write_job_t::read_next(bool first, bool more) {
	if (first) {
		// TODO: Use constructor or clear memory for request
		request_ = std::make_unique<request_t >();
	}
	if (!more) {
		push_request();
		return;
	}
	rpc_request_t rpc_request;
	async_reader_.Read(&rpc_request, this);
	deserialize_part(rpc_request, first, *request_, request_json_offset_, request_data_offset_);
}

void write_job_t::push_request() {
	if (request_json_offset_ != request_->json.size() || request_data_offset_ != request_->data.size()) {
		// TODO: no throw
		throw std::invalid_argument("Incomplete buffer");
	}
	request_->deadline = to_dnet_time(ctx_.deadline());

	// TODO add to req
	// auto responder = new responder_t(*this);

	auto req = new dnet_io_req;
	memset(req, 0, sizeof(dnet_io_req));
	req->st = node_.st;
	req->header = static_cast<void*>(&request_->cmd);
	req->n2_msg = request_.release();

	dnet_schedule_io(&node_, req);
}

void write_job_t::send_response(std::unique_ptr<response_t> response) {
	state_ = state_t::RESPONSE_COMPLETE;
	async_reader_.Finish(serialize(*response), ::grpc::Status::OK, this);
}

}} // namespace ioremap::grpc

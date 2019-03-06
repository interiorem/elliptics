#include "serialization.hpp"

namespace ioremap { namespace grpc {

fb_grpc_dnet::Time to_rpc_time(dnet_time time) {
	return fb_grpc_dnet::Time(time.tsec, time.tnsec);
}

dnet_time to_dnet_time(const fb_grpc_dnet::Time *time) {
	return dnet_time{time->sec(), time->nsec()};
}

dnet_time to_dnet_time(const std::chrono::system_clock::time_point &time_point) {
	using namespace std::chrono;

	auto sec = duration_cast<seconds>(time_point.time_since_epoch());
	auto nsec = duration_cast<nanoseconds>((time_point - sec).time_since_epoch());
	return {static_cast<uint64_t>(sec.count()), static_cast<uint64_t>(nsec.count())};
}

flatbuffers::Offset<fb_grpc_dnet::Cmd> serialize_cmd(flatbuffers::grpc::MessageBuilder &builder, const dnet_cmd &cmd) {
	auto cmd_id_id = std::any_of(cmd.id.id, cmd.id.id + DNET_ID_SIZE, [](uint8_t b){ return b; })
		         ? builder.CreateVector(cmd.id.id, DNET_ID_SIZE)
		         : flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{};

	return fb_grpc_dnet::CreateCmd(
		builder,
		cmd_id_id,
		cmd.id.group_id,
		cmd.status,
		cmd.backend_id,
		cmd.trace_id,
		cmd.flags,
		cmd.trans
	);
}

void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, dnet_cmd &cmd) {
	memset(&cmd, 0, sizeof(dnet_cmd));

	cmd.id.group_id = fb_cmd->group_id();
	cmd.status = fb_cmd->status();
	cmd.backend_id = fb_cmd->backend_id();
	cmd.trace_id = fb_cmd->trace_id();
	cmd.flags = fb_cmd->flags();
	cmd.trans = fb_cmd->trans();

	auto fb_cmd_id = fb_cmd->id();
	auto id_len = flatbuffers::VectorLength(fb_cmd_id);
	if (!id_len) {
		return;
	}
	if (id_len != DNET_ID_SIZE) {
		// TODO: no throw
		throw std::invalid_argument("Unexpected cmd.id size");
	}
	memcpy(cmd.id.id, fb_cmd_id->data(), DNET_ID_SIZE);
}

}} // namespace ioremap::grpc

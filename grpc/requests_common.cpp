#include "requests_common.hpp"

#include <algorithm>

namespace ioremap { namespace elliptics { namespace grpc_dnet {

flatbuffers::Offset<flatbuffers::Vector<uint8_t>> serialize_cmd_id_id(flatbuffers::grpc::MessageBuilder &builder,
                                                                      const uint8_t *id) {
	const uint8_t *id_end = id + DNET_ID_SIZE;
	if (std::find_if(id, id_end, [](uint8_t b){ return b != 0; }) != id_end) {
		return builder.CreateVector(id, DNET_ID_SIZE);
	} else {
		return 0;
	}
}

flatbuffers::Offset<fb_grpc_dnet::Cmd> serialize_cmd(flatbuffers::grpc::MessageBuilder &builder,
                                                     const dnet_cmd &cmd) {
	return fb_grpc_dnet::CreateCmd(
		builder,
		serialize_cmd_id_id(builder, cmd.id.id),
		cmd.id.group_id,
		cmd.status,
		cmd.cmd,
		cmd.backend_id,
		cmd.trace_id,
		cmd.flags,
		cmd.trans
	);
}

void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, dnet_cmd &cmd) {
	memset(&cmd, 0, sizeof(dnet_cmd));

	auto fb_cmd_id = fb_cmd->id();
	auto id_len = flatbuffers::VectorLength(fb_cmd_id);
	if (id_len) {
		if (id_len != DNET_ID_SIZE) {
			throw std::invalid_argument("Unexpected cmd.id size");
		}

		memcpy(cmd.id.id, fb_cmd_id->data(), DNET_ID_SIZE);

	} else {
		memset(cmd.id.id, 0, DNET_ID_SIZE);
	}

	cmd.id.group_id = fb_cmd->group_id();

	cmd.status = fb_cmd->status();
	cmd.cmd = fb_cmd->cmd();
	cmd.backend_id = fb_cmd->backend_id();
	cmd.trace_id = fb_cmd->trace_id();
	cmd.flags = fb_cmd->flags();
	cmd.trans = fb_cmd->trans();
}

void nanosec_to_dnet_time(uint64_t fb_time, dnet_time &time) {
	time.tsec = fb_time / 1000000000;
	time.tnsec = fb_time % 1000000000;
}

uint64_t dnet_time_to_nanosec(const dnet_time &time) {
	return time.tsec * 1000000000 + time.tnsec;
}

void time_point_to_dnet_time(const std::chrono::system_clock::time_point &time_point, dnet_time &time) {
	uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time_point.time_since_epoch()).count();
	nanosec_to_dnet_time(ns, time);
}

}}} // namespace ioremap::elliptics::grpc_dnet

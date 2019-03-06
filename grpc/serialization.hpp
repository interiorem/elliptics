#pragma once

#include "elliptics.grpc.fb.h"
#include "elliptics/packet.h"

namespace ioremap { namespace grpc {

const std::size_t GRPC_MAX_MESSAGE_SIZE =  4 * 1024 * 1024 /* 4Mb */ - 1024; /* flatbuffers overhead */;

fb_grpc_dnet::Time to_rpc_time(dnet_time time);
dnet_time to_dnet_time(const fb_grpc_dnet::Time *time);
dnet_time to_dnet_time(const std::chrono::system_clock::time_point &time_point);

flatbuffers::Offset<fb_grpc_dnet::Cmd> serialize_cmd(flatbuffers::grpc::MessageBuilder &builder, const dnet_cmd &cmd);
void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, dnet_cmd &cmd);

}} // namespace ioremap::grpc

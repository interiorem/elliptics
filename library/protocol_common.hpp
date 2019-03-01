#ifndef ELLIPTICS_PROTOCOL_COMMON_HPP
#define ELLIPTICS_PROTOCOL_COMMON_HPP

#include <elliptics/packet.h>
#include <elliptics/utils.hpp>

namespace ioremap { namespace elliptics {

#define DNET_READ_FLAGS_JSON (1<<0)
#define DNET_READ_FLAGS_DATA (1<<1)

static inline const char *dnet_dump_read_flags(uint64_t flags)
{
	static __thread char buffer[256];
	static struct flag_info infos[] = {
		{ DNET_READ_FLAGS_JSON, "json" },
		{ DNET_READ_FLAGS_DATA, "data" },
	};

	dnet_flags_dump_raw(buffer, sizeof(buffer), flags, infos, sizeof(infos) / sizeof(infos[0]));

	return buffer;
};

data_pointer serialize(const dnet_json_header &value);

void deserialize(const data_pointer &data, dnet_json_header &value);

}} // namespace ioremap::elliptics

#endif // ELLIPTICS_PROTOCOL_COMMON_HPP

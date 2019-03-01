#include "n2_protocol.h"
#include "n2_protocol.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

data_place::place_type data_place::where() const {
	return in_file.fd >= 0 ? IN_FILE : IN_MEMORY;
}

data_place data_place::from_file(const data_in_file &in_file) {
	return {
		.in_file = in_file,
		.in_memory = {},
	};
}

data_place data_place::from_memory(const data_pointer &in_memory) {
	return {
		.in_file = {},
		.in_memory = in_memory,
	};
}

}}} // namespace ioremap::elliptics::n2

extern "C" {

void n2_message_free(struct n2_message *n2_msg) {
	delete n2_msg;
}

struct dnet_cmd *n2_access_cmd(struct n2_message *n2_msg) {
	return &n2_msg->cmd;
}

} // extern "C"

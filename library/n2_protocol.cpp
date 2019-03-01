#include "n2_protocol.h"
#include "n2_protocol.hpp"

#include <blackhole/attribute.hpp>
#include <fcntl.h>

#include "access_context.h"
#include "common.hpp"
#include "elliptics.h"

namespace ioremap { namespace elliptics { namespace n2 {

void data_in_file::detach() {
	if (fd < 0)
		return;

	if ((on_exit & DNET_IO_REQ_FLAGS_CACHE_FORGET) && fsize)
		posix_fadvise(fd, local_offset, fsize, POSIX_FADV_DONTNEED);
	fd = -1;
}

data_pointer data_in_file::read_and_detach() {
	auto data = data_pointer::allocate(fsize);

	int err = dnet_read_ll(fd, data.data<char>(), fsize, local_offset);
	if (err) {
		throw std::runtime_error("Error while reading data: " + describe_errc(err));
	}

	detach();
	return data;
}

data_place::place_type data_place::where() const {
	return in_file.fd >= 0 ? IN_FILE : IN_MEMORY;
}

void data_place::force_memory() {
	if (where() == IN_FILE) {
		in_memory = in_file.read_and_detach();
	}
}

data_place data_place::from_file(data_in_file file_data) {
	return {
		.in_file = std::move(file_data),
		.in_memory = {},
	};
}

data_place data_place::from_memory(data_pointer memory_data) {
	return {
		.in_file = {},
		.in_memory = std::move(memory_data),
	};
}

int send_response(struct dnet_net_state *st, call *c, message *msg, struct dnet_access_context *context) {
	return n2_send_response(st, static_cast<n2_call *>(c), static_cast<n2_message *>(msg), context);
}

void read_response::make_owning() {
	data.force_memory();
}

}}} // namespace ioremap::elliptics::n2

extern "C" {

struct dnet_cmd *n2_message_access_cmd(struct n2_message *msg) {
	return &msg->cmd;
}

int n2_call_get_request(struct dnet_node *n, struct n2_call *call_data, struct n2_message **msg) {
	auto impl = [&]{
		*msg = static_cast<n2_message *>(call_data->get_request().release());
		return 0;
	};
	return c_exception_guard(impl, n, __FUNCTION__);
}

void n2_call_free(struct n2_call *call_data) {
	delete call_data;
}

} // extern "C"

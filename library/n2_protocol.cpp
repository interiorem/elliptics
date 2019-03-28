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

int send_response(struct dnet_net_state *st, struct dnet_cmd *cmd, std::function<void ()> response,
                  struct dnet_access_context *context) {
	auto resp_info = new n2_response_info({ *cmd, std::move(response) });
	return n2_send_response(st, resp_info, context);
}

}}} // namespace ioremap::elliptics::n2

extern "C" {

struct dnet_cmd *n2_request_info_access_cmd(struct n2_request_info *req_info) {
	return &req_info->request->cmd;
}

struct dnet_cmd *n2_response_info_access_cmd(struct n2_response_info *resp_info) {
	return &resp_info->cmd;
}

void n2_request_info_free(struct n2_request_info *req_info) {
	delete req_info;
}

void n2_response_info_free(struct n2_response_info *resp_info) {
	delete resp_info;
}

} // extern "C"

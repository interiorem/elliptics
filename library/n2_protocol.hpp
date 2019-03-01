#ifndef ELLIPTICS_N2_PROTOCOL_HPP
#define ELLIPTICS_N2_PROTOCOL_HPP

#include <optional>

#include "elliptics/packet.h"
#include "elliptics/utils.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

struct data_in_file {
	int fd = -1;
	off_t local_offset = 0;
	size_t fsize = 0;
	int on_exit = 0;

	void detach();
	data_pointer read_and_detach();
};

struct data_place {
	data_in_file in_file;
	data_pointer in_memory;

	enum place_type {
		IN_MEMORY,
		IN_FILE,
	};
	place_type where() const;

	void force_memory();

	static data_place from_file(data_in_file in_file);
	static data_place from_memory(data_pointer in_memory);
};

struct message {
	dnet_cmd cmd;

	virtual void make_owning() {}
	virtual ~message() = default;
};

class call {
public:
	/*
	 * To call on client side
	 */

	// must be called once, otherwise std::logic_error thrown
	virtual void request(std::unique_ptr<message> msg) = 0;

	// must be called once, otherwise std::logic_error thrown
	virtual std::unique_ptr<message> get_reply() = 0;

	virtual int get_reply_error() = 0;

	/*
	 * To call on server side
	 */

	// must be called once, otherwise std::logic_error thrown
	virtual std::unique_ptr<message> get_request() = 0;

	// reply or reply_error (only one of them) must be called once, otherwise std::logic_error thrown
	virtual void reply(std::unique_ptr<message> msg) = 0;
	virtual void reply_error(int errc) = 0;

	virtual ~call() = default;
};

int send_response(struct dnet_net_state *st, call *c, message *msg, struct dnet_access_context *context);

}}} // namespace ioremap::elliptics::n2

// All requests/responses must be inherited from n2_message;
// common_request is visible from dnet_io_req. Since this struct
// is visible from C code, it is declared out of namespace.
struct n2_message : ioremap::elliptics::n2::message {};
struct n2_call : public ioremap::elliptics::n2::call {};

namespace ioremap { namespace elliptics { namespace n2 {

// TODO(sabramkin): add constructors to *_request structs (since {}-initialization has broken)

struct read_request : n2_message {
	uint64_t ioflags;
	uint64_t read_flags;
	uint64_t data_offset;
	uint64_t data_size;

	dnet_time deadline;
};

struct read_response : n2_message {
	uint64_t record_flags;
	uint64_t user_flags;

	dnet_time json_timestamp;
	uint64_t json_size;
	uint64_t json_capacity;
	uint64_t read_json_size;
	data_pointer json;

	dnet_time data_timestamp;
	uint64_t data_size;
	uint64_t read_data_offset;
	uint64_t read_data_size;
	data_place data;

	void make_owning() override;
};

struct write_request : n2_message {
	uint64_t ioflags;
	uint64_t user_flags;

	uint64_t json_size;
	uint64_t json_capacity;
	dnet_time json_timestamp;
	data_pointer json;

	uint64_t data_offset;
	uint64_t data_size;
	uint64_t data_capacity;
	dnet_time data_timestamp;
	uint64_t data_commit_size;
	data_pointer data;

	uint64_t cache_lifetime;

	dnet_time deadline;
};

struct lookup_response : n2_message {
	uint64_t record_flags;
	uint64_t user_flags;
	std::string path;

	dnet_time json_timestamp;
	uint64_t json_offset;
	uint64_t json_size;
	uint64_t json_capacity;
	std::vector<unsigned char> json_checksum;

	dnet_time data_timestamp;
	uint64_t data_offset;
	uint64_t data_size;
	std::vector<unsigned char> data_checksum;
};

}}} // namespace ioremap::elliptics::n2

#endif // ELLIPTICS_N2_PROTOCOL_HPP

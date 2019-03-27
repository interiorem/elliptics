#ifndef ELLIPTICS_N2_PROTOCOL_HPP
#define ELLIPTICS_N2_PROTOCOL_HPP

#include "elliptics/packet.h"
#include "elliptics/utils.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

struct data_in_file {
	int fd = -1;
	off_t local_offset = 0;
	size_t fsize = 0;
	int on_exit = 0;
};

struct data_place {
	enum place_type {
		IN_MEMORY,
		IN_FILE,
	};

	data_in_file in_file;
	data_pointer in_memory;

	place_type where() const;

	static data_place from_file(const data_in_file &in_file);
	static data_place from_memory(const data_pointer &in_memory);
};

struct message {
	dnet_cmd cmd;

	virtual void make_owning() {}
	virtual ~message() = default;
};

}}} // namespace ioremap::elliptics::n2

// All requests/responses must be inherited from n2_message;
// common_request is visible from dnet_io_req. Since this struct
// is visible from C code, it is declared out of namespace.
struct n2_message : ioremap::elliptics::n2::message {};

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

	virtual void make_owning() override { /*TODO(sabramkin): implement*/ }
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

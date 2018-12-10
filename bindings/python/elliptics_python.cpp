/*
 * 2008+ Copyright (c) Evgeniy Polyakov <zbr@ioremap.net>
 * 2013+ Copyright (c) Kirill Smorodinnikov <shaitkir@gmail.com>
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 */

#include <blackhole/wrapper.hpp>

#include <blackhole/attribute.hpp>
#include <blackhole/builder.hpp>
#include <blackhole/extensions/writer.hpp>
#include <blackhole/formatter/string.hpp>
#include <blackhole/formatter/tskv.hpp>
#include <blackhole/handler/blocking.hpp>
#include <blackhole/logger.hpp>
#include <blackhole/record.hpp>
#include <blackhole/root.hpp>
#include <blackhole/sink/asynchronous.hpp>
#include <blackhole/sink/file.hpp>

#include "elliptics/cppdef.h"
#include "library/logger.hpp"

#include "python.hpp"
#include "elliptics_id.h"
#include "async_result.h"
#include "result_entry.h"
#include "elliptics_time.h"
#include "elliptics_io_attr.h"
#include "elliptics_session.h"

namespace bp = boost::python;

namespace ioremap { namespace elliptics { namespace python {
// TODO: rewrite enums below with enum class and move them into python.hpp
enum elliptics_iterator_types {
	itype_disk	= DNET_ITYPE_DISK,
	itype_network	= DNET_ITYPE_NETWORK,
};

enum elliptics_iterator_flags {
	iflag_default		= 0,
	iflag_data		= DNET_IFLAGS_DATA,
	iflag_key_range		= DNET_IFLAGS_KEY_RANGE,
	iflag_ts_range		= DNET_IFLAGS_TS_RANGE,
	iflag_no_meta		= DNET_IFLAGS_NO_META,
	iflags_move		= DNET_IFLAGS_MOVE,
	iflags_overwrite	= DNET_IFLAGS_OVERWRITE,
	iflags_json		= DNET_IFLAGS_JSON
};

enum elliptics_cflags {
	cflags_default	= 0,
	cflags_direct	= DNET_FLAGS_DIRECT,
	cflags_nolock	= DNET_FLAGS_NOLOCK,
	cflags_checksum = DNET_FLAGS_CHECKSUM,
	cflags_nocache  = DNET_FLAGS_NOCACHE,
	cflags_no_queue_timeout	= DNET_FLAGS_NO_QUEUE_TIMEOUT
};

enum elliptics_ioflags {
	ioflags_default			= 0,
	ioflags_append			= DNET_IO_FLAGS_APPEND,
	ioflags_prepare			= DNET_IO_FLAGS_PREPARE,
	ioflags_commit			= DNET_IO_FLAGS_COMMIT,
	ioflags_overwrite		= DNET_IO_FLAGS_OVERWRITE,
	ioflags_nocsum			= DNET_IO_FLAGS_NOCSUM,
	ioflags_plain_write		= DNET_IO_FLAGS_PLAIN_WRITE,
	ioflags_nodata			= DNET_IO_FLAGS_NODATA,
	ioflags_cache			= DNET_IO_FLAGS_CACHE,
	ioflags_cache_only		= DNET_IO_FLAGS_CACHE_ONLY,
	ioflags_cache_remove_from_disk	= DNET_IO_FLAGS_CACHE_REMOVE_FROM_DISK,
	ioflags_cas_timestamp		= DNET_IO_FLAGS_CAS_TIMESTAMP,
	ioflags_mix_states		= DNET_IO_FLAGS_MIX_STATES,
};

enum elliptics_record_flags {
	record_flags_remove		= DNET_RECORD_FLAGS_REMOVE,
	record_flags_nocsum		= DNET_RECORD_FLAGS_NOCSUM,
	record_flags_append		= DNET_RECORD_FLAGS_APPEND,
	record_flags_exthdr		= DNET_RECORD_FLAGS_EXTHDR,
	record_flags_uncommitted	= DNET_RECORD_FLAGS_UNCOMMITTED,
	record_flags_chunked_csum	= DNET_RECORD_FLAGS_CHUNKED_CSUM,
	record_flags_corrupted		= DNET_RECORD_FLAGS_CORRUPTED,
};

enum elliptics_exceptions_policy {
	policy_no_exceptions			= ioremap::elliptics::session::no_exceptions,
	policy_throw_at_start			= ioremap::elliptics::session::throw_at_start,
	policy_throw_at_wait			= ioremap::elliptics::session::throw_at_wait,
	policy_throw_at_get			= ioremap::elliptics::session::throw_at_get,
	policy_throw_at_iterator_end		= ioremap::elliptics::session::throw_at_iterator_end,
	policy_default_exceptions		= ioremap::elliptics::session::throw_at_wait |
						  ioremap::elliptics::session::throw_at_get |
						  ioremap::elliptics::session::throw_at_iterator_end
};

enum elliptics_config_flags {
	config_flags_join_network		= DNET_CFG_JOIN_NETWORK,
	config_flags_no_route_list		= DNET_CFG_NO_ROUTE_LIST,
	config_flags_mix_states			= DNET_CFG_MIX_STATES,
	config_flags_no_csum			= DNET_CFG_NO_CSUM,
	config_flags_randomize_states		= DNET_CFG_RANDOMIZE_STATES,
};

enum elliptics_node_status_flags {
	node_status_flags_change		= DNET_ATTR_STATUS_CHANGE,
	node_status_flags_exit			= DNET_STATUS_EXIT,
	node_status_flags_ro			= DNET_STATUS_RO,
};

dnet_config* dnet_config_init() {
	auto config = new dnet_config();
	memset(config, 0, sizeof(dnet_config));
	config->wait_timeout = 5;
	config->check_timeout = 20;
	config->io_thread_num = 1;
	config->net_thread_num = 1;
	config->nonblocking_io_thread_num = 1;
	return config;
}

void dnet_config_set_cookie(dnet_config &config, const std::string &cookie) {
	size_t sz = sizeof(config.cookie);
	if (cookie.size() + 1 < sz)
		sz = cookie.size() + 1;
	memset(config.cookie, 0, sizeof(config.cookie));
	snprintf(config.cookie, sz, "%s", (char *)cookie.data());
}

std::string dnet_config_get_cookie(const dnet_config &config) {
	return std::string(config.cookie, sizeof(config.cookie));
}

static std::unique_ptr<blackhole::formatter_t> make_string_formatter() {
	static const std::string pattern = "{timestamp:l} {trace_id:{0:default}0>16}/{thread:d}/{process} "
	                                   "{severity}: {message}, attrs: [{...}]";

	static auto sevmap = [](std::size_t severity, const std::string &spec, blackhole::writer_t &writer) {
		static const std::array<const char *, 5> mapping = {
		{"DEBUG", "NOTICE", "INFO", "WARNING", "ERROR"}};
		if (severity < mapping.size()) {
			writer.write(spec, mapping[severity]);
		} else {
			writer.write(spec, severity);
		}
	};

	blackhole::builder<blackhole::formatter::string_t> builder(pattern);
	builder.mapping(sevmap);
	return std::move(builder).build();
}

static std::unique_ptr<blackhole::formatter_t> make_tskv_formatter() {
	blackhole::builder<blackhole::formatter::tskv_t> builder;
	return std::move(builder).build();
}

std::unique_ptr<dnet_logger> make_logger(const std::string &file, int level, bool watched, bool tskv) {
	auto formatter = tskv ? make_tskv_formatter() : make_string_formatter();

	auto file_sink = [&] {
		blackhole::builder <blackhole::sink::file_t> builder(file);
		builder.flush_every(1);
		if (watched)
			builder.rotate_checking_stat();
		return std::move(builder).build();
	}();

	auto async_sink = [&] {
		blackhole::builder<blackhole::sink::asynchronous_t> builder(std::move(file_sink));
		builder.factor(20);
		builder.wait();
		return std::move(builder).build();
	}();

	std::vector<std::unique_ptr<blackhole::handler_t>> handlers;
	handlers.emplace_back(
		blackhole::builder<blackhole::handler::blocking_t>()
			.set(std::move(formatter))
			.add(std::move(async_sink))
			.build()
	);

	std::unique_ptr<blackhole::root_logger_t> logger(new blackhole::root_logger_t(std::move(handlers)));

	logger->filter([level](const blackhole::record_t &record) {
		return log_filter(record.severity(), level);
	});

	return std::move(logger);
}

class logger {
public:
	logger(const std::string &file, int level, bool watched = false, bool tskv = false)
	: m_logger(make_logger(file, dnet_log_level(level), watched, tskv)) {}

	std::unique_ptr<dnet_logger> get_logger() {
		return std::unique_ptr<dnet_logger>(new blackhole::wrapper_t(*m_logger, {}));
	}

private:
	std::unique_ptr<dnet_logger> m_logger;
};

class elliptics_node_python : public node, public bp::wrapper<node> {
public:
	elliptics_node_python(logger &log)
	: node(log.get_logger()) {}

	elliptics_node_python(logger &log, logger &access_log)
	: node(log.get_logger(), access_log.get_logger()) {}

	elliptics_node_python(logger &log, dnet_config &cfg)
	: node(log.get_logger(), cfg) {}

	elliptics_node_python(logger &log, logger &access_log, dnet_config &cfg)
	: node(log.get_logger(), access_log.get_logger(), cfg) {}

	elliptics_node_python(const node &n)
	: node(n) {}

	~elliptics_node_python() {
		py_allow_threads_scoped pythr;
		m_data.reset();
	}

	void add_remotes(const bp::api::object &remotes) {
		auto remotes_len = bp::len(remotes);
		std::vector<address> std_remotes;
		std_remotes.reserve(remotes_len);

		for (bp::stl_input_iterator<bp::tuple> it(remotes), end; it != end; ++it) {
			bp::extract<std::string> get_host((*it)[0]);
			bp::extract<int> get_port((*it)[1]);
			bp::extract<int> get_family((*it)[2]);
			std_remotes.emplace_back(get_host(), get_port(), get_family());
		}

		py_allow_threads_scoped pythr;
		add_remote(std_remotes);
	}
};


class elliptics_error_translator
{
	public:
		elliptics_error_translator()
		{}

		void operator() (const error &err) const {
			bp::api::object exception(err);
			bp::api::object type = m_type;
			for (size_t i = 0; i < m_types.size(); ++i) {
				if (m_types[i].first == err.error_code()) {
					type = m_types[i].second;
					break;
				}
			}
			PyErr_SetObject(type.ptr(), exception.ptr());
		}

		void initialize() {
			m_type = new_exception("Error");
			register_type(-ENOENT, "NotFoundError");
			register_type(-ETIMEDOUT, "TimeoutError");
		}

		void register_type(int code, const char *name) {
			register_type(code, new_exception(name, m_type.ptr()));
		}

		void register_type(int code, const bp::api::object &type) {
			m_types.push_back(std::make_pair(code, type));
		}

	private:
		bp::api::object new_exception(const char *name, PyObject *parent = NULL) {
			std::string scopeName = bp::extract<std::string>(bp::scope().attr("__name__"));
			std::string qualifiedName = scopeName + "." + name;

			PyObject *type = PyErr_NewException(&qualifiedName[0], parent, 0);
			if (!type)
				bp::throw_error_already_set();
			bp::api::object type_object = bp::api::object(bp::handle<>(type));
			bp::scope().attr(name) = type_object;
			return type_object;
		}

		bp::api::object m_type;
		std::vector<std::pair<int, bp::api::object> > m_types;
};

void ios_base_failure_translator(const std::ios_base::failure &exc)
{
	PyErr_SetString(PyExc_IOError, exc.what());
}

void next_impl(bp::api::object &value, const bp::api::object &next)
{
	value = next();
}

std::string get_cmd_string(int cmd) {
	return std::string(dnet_cmd_string(cmd));
}

BOOST_PYTHON_MODULE(core) {
	PyEval_InitThreads();
	bp::docstring_options local_docstring_options(true, false, false);
	bp::class_<error>(
	    "ErrorInfo", "Basic error for Elliptics",
	    bp::init<int, std::string>())
		.def("__str__", &error::error_message,
		    "__str__()\n"
		    "    x.__str__() <==> str(x)")
		.def("__repr__", &error::error_message,
		     "__repr__()\n"
		     "    x.__repr__() <==> repr(x)")
		.add_property("message", &error::error_message,
		    "message()\n"
		    "    Returns description of the message\n\n"
		    "    print error.message()")
		.add_property("code", &error::error_code,
		    "code()\n"
		    "    Returns code of the error\n\n"
		    "    print error.code()")
	;
	elliptics_error_translator error_translator;
	error_translator.initialize();

	bp::register_exception_translator<timeout_error>(error_translator);
	bp::register_exception_translator<not_found_error>(error_translator);
	bp::register_exception_translator<error>(error_translator);
	bp::register_exception_translator<std::ios_base::failure>(ios_base_failure_translator);

	bp::class_<logger, boost::noncopyable>(
		"Logger", "File logger for using inside Elliptics client library",
		bp::init<const std::string &, int, bool, bool>(
			bp::args("log_file", "log_level", "watched", "tskv"),
		    "__init__(self, filename, log_level, tskv)\n"
		    "    Initializes file logger by the specified file and level of verbosity\n\n"
		    "    logger = elliptics.Logger(\"/dev/stderr\", elliptics.log_level.debug)"));

	bp::class_<dnet_config>(
	    "Config", "Config allows override default configuration for client node")
		.def("__init__", boost::python::make_constructor(&dnet_config_init))
		.add_property("cookie", &dnet_config_get_cookie, &dnet_config_set_cookie,
		              "authentication cookie")
		.def_readwrite("wait_timeout", &dnet_config::wait_timeout,
		               "Time to wait for an operation complete")
		.def_readwrite("check_timeout", &dnet_config::check_timeout,
		               "Timeout for pinging node")
		.def_readwrite("io_thread_num", &dnet_config::io_thread_num,
		               "Number of IO threads in processing pool")
		.def_readwrite("nonblocking_io_thread_num", &dnet_config::nonblocking_io_thread_num,
		               "Number of IO threads in processing pool dedicated to nonblocking operations")
		.def_readwrite("net_thread_num", &dnet_config::net_thread_num,
		               "Number of threads in network processing pool")
		.def_readwrite("flags", &dnet_config::flags,
		               "Bit set of elliptics.config_flags")
		.def_readwrite("client_prio", &dnet_config::client_prio,
		               "IP priority")
	;

	bp::class_<elliptics_node_python, boost::noncopyable>("Node", "Node represents a connection with Elliptics",
	                                                      bp::init<logger &>(bp::args("logger")))
		.def(bp::init<logger &, dnet_config &>(bp::args("logger", "config")))
		.def(bp::init<logger &, logger &>(bp::args("logger", "access_logger")))
		.def(bp::init<logger &, logger &, dnet_config &>(bp::args("logger", "access_logger", "config")))
		.def("add_remotes", &elliptics_node_python::add_remotes,
		     (bp::arg("remotes")),
		     "add_remotes(remotes)\n"
		     "    Adds connections to Elliptics node\n"
		     "    which located on remotes.\n\n"
		     "    node.add_remotes(('host.com:1025:2'))")
		.def("set_timeouts", static_cast<void (node::*)(const int, const int)>(&node::set_timeouts),
		     (bp::arg("wait_timeout"), bp::arg("check_timeout")),
		     "set_timeouts(wait_timeout, check_timeout)\n"
		     "    Changes timeouts values\n\n"
		     "    node.set_timeouts(wait_timeout=5, check_timeout=50)")
		.def("set_keepalive", &node::set_keepalive,
		     (bp::arg("idle"), bp::arg("cnt"), bp::arg("interval")),
		     "set_keepalive(idle, cnt, interval)\n"
		     "    Sets tcp keepalive parameters to connections\n")
	;

	bp::enum_<elliptics_iterator_flags>("iterator_flags",
	    "Flags which specifies how iteration should be performed:\n\n"
	    "default\n    There no filtering should be while iteration. All keys will be presented\n"
	    "data\n    Iteration results should also includes objects datas\n"
	    "key_range\n    elliptics.Id ranges should be used for filtering keys on the node while iteration\n"
	    "ts_range\n    Time range should be used for filtering keys on the node while iteration\n"
	    "no_meta\n    Iteration results will have empty key's metadata (user_flags and timestamp)\n"
	    "move\n    Server-send iterator should move data not copy. This will force iterator/server-send logic\n"
	    "          to queue REMOVE command locally if remote write has succeeded.\n"
	    "overwrite\n    Overwrite data. If this flag is NOT set, we only write data if remote timestamp is less\n"
	    "               than in data being written. When NOT set, data will still be transferred over the network,\n"
	    "               even if remote timestamp doesn't allow us to overwrite data.\n"
	    "json\n    Iteration results should also includes objects json")
		.value("default", iflag_default)
		.value("data", iflag_data)
		.value("key_range", iflag_key_range)
		.value("ts_range", iflag_ts_range)
		.value("no_meta", iflag_no_meta)
		.value("move", iflags_move)
		.value("overwrite", iflags_overwrite)
		.value("json", iflags_json)
	;

	bp::enum_<elliptics_iterator_types>("iterator_types",
	    "Flags which specifies how iteration results should be transmitted:\n\n"
	    "disk\n    Iterator saves data chunks (index/metadata + (optionally) data)\n"
	    "          locally on server to $root/iter/$id instead of sending chunks to client\n"
	    "network\n    Iterator sends data chunks to client")
		.value("disk", itype_disk)
		.value("network", itype_network)
	;

	bp::enum_<elliptics_cflags>("command_flags",
	    "Flags which specifies how operation should be done\n\n"
	    "default\n    The key is locked before performing an operation and unlocked when an operation will done\n"
	    "direct\n    Request is sent to the specified Node bypassing the DHT ring\n"
	    "nolock\n    Server will not check the key is locked and will not lock it during this transaction.\n"
	    "            The operation will be handled in separated io thread pool\n"
	    "checksum\n  Only valid flag for LOOKUP command - when set, return checksum in file_info structure\n"
	    "nocache\n   Currently only valid flag for LOOKUP command - when set, don't check fileinfo in cache\n"
	    "no_queue_timeout\n Do not check queue timeout for this operation\n")
		.value("default", cflags_default)
		.value("direct", cflags_direct)
		.value("nolock", cflags_nolock)
		.value("checksum", cflags_checksum)
		.value("nocache", cflags_nocache)
		.value("no_queue_timeout", cflags_no_queue_timeout)
	;

	bp::enum_<elliptics_ioflags>("io_flags",
		"Bit flags which specifies how operation should be executed:\n\n"
		"default\n    The default value overwrites the data by specified offset and size\n"
		"append\n    Append given data at the end of the object\n"
		"prepare\n    eblob prepare/commit phase \n"
		"commit\n    eblob prepare/commit phase \n"
		"overwrite\n    Overwrite data \n"
		"nocsum\n    Do not checksum data \n"
		"plain_write\n    This flag is used when we want backend not to perform any additional actions\n"
		             "    except than write data at given offset\n"
		"nodata\n    Do not really send data in range request.\n"
		"            Send only statistics instead.\n"
		"cache\n    Says we should first check cache: read/write or delete\n"
		"cache_only\n    Means we do not want to sink to disk,\n"
		"                just return whatever cache processing returned (even error)\n"
		"cache_remove_from_disk\n    is set and object is being removed from cache,\n"
		"                            then remove object from disk too"
		"cas_timestamp\n    When set, write will only succeed if data timestamp is higher than timestamp stored on disk\n"
		"mix_states\n    Read request with this flag forces replica selection according to their weights\n")
		.value("default", ioflags_default)
		.value("append", ioflags_append)
		.value("prepare", ioflags_prepare)
		.value("commit", ioflags_commit)
		.value("overwrite", ioflags_overwrite)
		.value("nocsum", ioflags_nocsum)
		.value("plain_write", ioflags_plain_write)
		.value("nodata", ioflags_nodata)
		.value("cache", ioflags_cache)
		.value("cache_only", ioflags_cache_only)
		.value("cache_remove_from_disk", ioflags_cache_remove_from_disk)
		.value("cas_timestamp", ioflags_cas_timestamp)
		.value("mix_states", ioflags_mix_states)
	;

	bp::enum_<elliptics_record_flags>("record_flags",
		"Bit flags which specifies state of the record at the backend\n\n"
		"remove\n    The record is removed\n"
		"nocsum\n    The record is written without csum\n"
		"append\n    The record is written via append\n"
		"exthdr\n    The record is written with extended header\n"
		"uncommitted\n    The record is uncommitted so it can't be read but can be written and committed\n"
		"chunked_csum\n    The record is checksummed by chunks\n"
		"corrupted\n    The record was corrupted")
		.value("remove", record_flags_remove)
		.value("nocsum", record_flags_nocsum)
		.value("append", record_flags_append)
		.value("exthdr", record_flags_exthdr)
		.value("uncommitted", record_flags_uncommitted)
		.value("chunked_csum", record_flags_chunked_csum)
		.value("corrupted", record_flags_corrupted)
	;

	bp::enum_<int>("log_level",
	    "Different levels of verbosity elliptics logs:\n\n"
	     "error\n    The level contains critical errors that materially affect the work\n"
	     "warning\n    The level contains reports of the previous level and warnings that may not affect the work\n"
	     "info\n    The level contains reports of the previous level and messages about the time of the various operations\n"
	     "notice\n    The level is considered to be the first level of debugging\n"
	     "debug\n    The level includes all sort of information about errors and work")
		.value("error", DNET_LOG_ERROR)
		.value("warning", DNET_LOG_WARNING)
		.value("info", DNET_LOG_INFO)
		.value("notice", DNET_LOG_NOTICE)
		.value("debug", DNET_LOG_DEBUG)
	;

	bp::enum_<elliptics_exceptions_policy>("exceptions_policy",
	    "Bit flags for specifying exception policy of elliptics.Session:\n\n"
	    "no_exceptions\n    No excetion will be thrown\n"
	    "throw_at_start\n    An exception will be thrown when the operation is started\n"
	    "throw_at_wait\n    An exception will be thrown when all results of the operation is available\n"
	    "throw_at_get\n    An exception will be thrown when the next result of the operation is available\n"
	    "throw_at_iterator_end\n    An exception will be thrown after the iterator has reached last result\n"
	    "default_exceptions\n    It is the union of follow flags: throw_at_wait, throw_at_get, throw_at_iterator_end")
		.value("no_exceptions", policy_no_exceptions)
		.value("throw_at_start", policy_throw_at_start)
		.value("throw_at_wait", policy_throw_at_wait)
		.value("throw_at_get", policy_throw_at_get)
		.value("throw_at_iterator_end", policy_throw_at_iterator_end)
		.value("default_exceptions", policy_default_exceptions)
	;

	bp::enum_<elliptics_config_flags>("config_flags",
	    "Bit flags which could be used at elliptics.Config.flags:\n\n"
	    "no_route_list\n    Do not request route table from remote nodes\n"
	    "mix_states\n    Mix states according to their weights before reading data\n"
	    "no_csum\n    Globally disable checksum verification and update\n"
	    "randomize_states\n    Randomize states for read requests\n\n"
	    "config.flags = elliptics.config_flags.mix_stats | elliptics.config_flags.randomize_states\n"
	    )
		.value("no_route_list", config_flags_no_route_list)
		.value("mix_states", config_flags_mix_states)
		.value("no_csum", config_flags_no_csum)
		.value("randomize_states", config_flags_randomize_states)
	;

	bp::enum_<elliptics_node_status_flags>("status_flags",
	    "Bit flags which used for changing node status:\n\n"
	    "change\n    Elliptics node status - if set, status will be changed\n"
	    "exit\n    Elliptics node should exit\n"
	    "ro\n    Elliptics node goes ro/rw")
		.value("change", node_status_flags_change)
		.value("exit", node_status_flags_exit)
		.value("ro", node_status_flags_ro)
	;

	bp::enum_<defrag_state>("defrag_state",
	                        "All possible states of defragmentation\n\n"
	                        "not_started\n    Defragmentation is not started\n"
	                        "data_sort\n    Data sort is in progress\n"
	                        "index_sort\n    Index sort is in progress\n"
	                        "compact\n    Compact is in progress")
		.value("not_started", defrag_state::NOT_STARTED)
		.value("data_sort", defrag_state::DATA_SORT)
		.value("index_sort", defrag_state::INDEX_SORT)
		.value("compact", defrag_state::COMPACT)
	;

	bp::enum_<inspect_state>("inspect_state",
	                         "All possible states of inspection\n\n"
	                         "not_started\n    Inspection is not started\n"
	                         "in_progress\n    Inspection is in progress")
	.value("not_started", inspect_state::NOT_STARTED)
	.value("in_progress", inspect_state::IN_PROGRESS)
	;

	init_elliptics_id();
	init_async_results();
	init_result_entry();
	init_elliptics_time();
	init_elliptics_io_attr();
	init_elliptics_session();
};

} } } // namespace ioremap::elliptics::python

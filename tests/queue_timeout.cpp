/*
 * 2013+ Copyright (c) Ruslan Nigmatullin <euroelessar@yandex.ru>
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

#include "test_base.hpp"

#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <boost/program_options.hpp>

#include "bindings/cpp/timer.hpp"

using namespace ioremap::elliptics;
using namespace boost::unit_test;

namespace tests {

constexpr int group = 1;
constexpr int backend_id = 1;

constexpr int group_with_overridden_queue_timeout = 2;
constexpr int backend_with_overridden_queue_timeout = 2;

static nodes_data::ptr configure_test_setup(const std::string &path) {
	std::vector<server_config> servers {
		[] () {
			auto ret = server_config::default_value();
			ret.options
				("io_thread_num", 1)
				("nonblocking_io_thread_num", 1)
				("net_thread_num", 1)
				("caches_number", 1)
				("queue_timeout", "1")
			;

			ret.backends.resize(2, ret.backends.front());

			ret.backends[0]
				("backend_id", backend_id)
				("enable", true)
				("group", group)
			;
			ret.backends[1]
				("backend_id", backend_with_overridden_queue_timeout)
				("enable", true)
				("group", group_with_overridden_queue_timeout)
				("queue_timeout", "2")
			;
			return ret;
		} ()
	};

	start_nodes_config start_config(results_reporter::get_stream(), std::move(servers), path);
	start_config.fork = true;

	return start_nodes(start_config);
}

/* The test validates dropping request on server-side after 1 seconds by follow:
 * * write test key
 * * set backend delay to 1,5 seconds to make the backend sleep 1,5 seconds before handling request
 * * sequentially sends 2 async read of written key commands with 5 seconds timeout:
 *   * the first read command will be taken by the only io thread that will sleep 1,5 on the backend delay
 *   * the second read command will be in io queue because the only io thread is busy handling the first command
 * * check that first command has been succeeded because 1,5 seconds delay on the backend fits 5 seconds timeout
 * * check that second command has been failed with timeout error because it was dropped on server-side due to
 *   queue timeout - it has spent about 1,5 seconds in io queue while the only io thread slept on the backend delay
 * * send another read command and check that it is succeeded - there should be no aftereffect.
 */
static void test_queue_timeout(session &s, const nodes_data *setup) {
	// check that test have only one node
	BOOST_REQUIRE(setup->nodes.size() == 1);

	// test key and data
	std::string key = "queue timeout test key";
	std::string data = "queue timeout test data";

	s.set_trace_id(rand());
	// write test key/data
	ELLIPTICS_REQUIRE(async_write, s.write_data(key, data, 0));

	const auto &node = setup->nodes.front();
	// sets 1,5 seconds delay to the only backend on the only node
	constexpr uint64_t delay_ms = 1500;
	s.set_delay(node.remote(), backend_id, delay_ms).get();

	// sets 5 seconds timeout - it should fit at least 2 x backend delay because
	// if the second command won't be dropped due to queue timeout its handling time
	// should be around 3 seconds (2 x backend delay).
	s.set_timeout(5);
	// first read command. It will hold the only io thread on 1,5 seconds backend delay.
	auto async = s.read_data(key, 0, 0);
	// second read command. It will be in io queue while the only io thread will sleep on backend delay.
	auto async_timeouted = s.read_data(key, 0, 0);
	{
		// first read command should be succeeded
		ELLIPTICS_COMPARE_REQUIRE(res, std::move(async), data);
	} {
		// second read command should be failed with timeout error due to queue timeout
		ELLIPTICS_REQUIRE_ERROR(res, std::move(async_timeouted), -ETIMEDOUT);
	} {
		// there should be no aftereffect, so next read request should be succeeded
		ELLIPTICS_COMPARE_REQUIRE(res, s.read_data(key, 0, 0), data);
	}
}

/* The test checks for error reply on dropped request, so client could be informed on timeout earlier,
 * not on session timeout.
 */
static void test_queue_ack_timeout(session &s, const nodes_data *setup) {
	// check that test have only one node
	BOOST_REQUIRE(setup->nodes.size() == 1);

	// test key and data
	std::string key = "queue timeout reply with timout test key";

	const auto &node = setup->nodes.front();
	// sets 2 seconds delay to the only backend on the only node
	constexpr uint64_t delay_ms = 2000;
	s.set_delay(node.remote(), backend_id, delay_ms).get();

	// Make sufficient large client session timeout (10 seconds) to test the situation when first request was processed
	// with `delay_ms` delay and second was dropped with ack as it's waiting time exceeds queue_timeout (1 second),
	// so client would be informed earlier, before session expiration. Client should be informed instanly after `delta_ms`,
	// plus some neglectable small time.
	s.set_timeout(10);
	s.set_filter(ioremap::elliptics::filters::all_with_ack);
	ioremap::elliptics::util::steady_timer timer;

	// first lookup will delay after decoupling from queue for `delay_ms`.
	auto async = s.lookup(key);
	// second lookup command. It will be in io queue while the only io thread will sleep on backend delay.
	auto async_timeouted = s.lookup(key);
	{
		ELLIPTICS_REQUIRE_ERROR(res, std::move(async), -ENOENT);
	} {
		// second lookup command should fail with timeout error due to drop from server queue request.
		ELLIPTICS_REQUIRE_ERROR(res, std::move(async_timeouted), -ETIMEDOUT);
		BOOST_REQUIRE(res.get().front().command()->flags & DNET_FLAGS_REPLY);

		const auto delta_ms = timer.get_ms();
		// Warning: all timings comparsion is a "danger zone", as execution time could depends on many factors.
		BOOST_REQUIRE_GE(delta_ms, delay_ms);
		BOOST_REQUIRE_LT(delta_ms, 2 * delay_ms);
	}
}


/* Idea is the same as @test_queue_timeout but it uses backend with overridden to 2 seconds queue timeout.
 * It consists of 2 parts:
 * * first part checks that overridden queue timeout is really overridden and
     is greater than 1 second (global queue timeout)
 * * second part checks that overridden queue timeout is about ~2 seconds.
 */
static void test_overridden_queue_timeout(session &s, const nodes_data *setup) {
	// check that test have only one node
	BOOST_REQUIRE(setup->nodes.size() == 1);

	// test key and data
	std::string key = "overridden queue timeout test key";
	std::string data = "overridden queue timeout test data";

	// write test key/data
	ELLIPTICS_REQUIRE(async_write, s.write_data(key, data, 0));

	const auto &node = setup->nodes.front();

	// First part.
	{
		// sets 1,5 seconds delay to the backend with overridden queue timeout on the only node
		s.set_delay(node.remote(), backend_with_overridden_queue_timeout, 1500).get();

		// sets 5 seconds timeout - it should fit at least 2 x backend delay because
		// the second command shouldn't be dropped due to queue timeout and its handling time
		// should be around 3 seconds (2 x backend delay).
		s.set_timeout(5);
		// first read command. It will hold the only io thread on 1,5 seconds backend delay.
		auto async = s.read_data(key, 0, 0);
		// second read command. It will be in io queue while the only io thread will sleep on backend delay.
		auto async2 = s.read_data(key, 0, 0);
		{
			// first read command should be succeeded
			ELLIPTICS_COMPARE_REQUIRE(res, std::move(async), data);
		} {
			// second read command should be succeeded since queue timeout is overridden to 2 seconds
			ELLIPTICS_COMPARE_REQUIRE(res, std::move(async2), data);
		}
	}

	// Second part.
	{
		// sets 2,5 seconds delay to the backend with overridden queue timeout
		s.set_delay(node.remote(), backend_with_overridden_queue_timeout, 2500).get();

		// sets 6 seconds timeout - it should fit at least 2 x backend delay because
		// if the second command won't be dropped due to queue timeout its handling time
		// should be around 5 seconds (2 x backend delay).
		s.set_timeout(6);

		// first read command. It will hold the only io thread on 2,5 seconds backend delay.
		auto async = s.read_data(key, 0, 0);
		// second read command. It will be in io queue while the only io thread will sleep on backend delay.
		auto async_timeouted = s.read_data(key, 0, 0);
		{
			// first read command should be succeeded
			ELLIPTICS_COMPARE_REQUIRE(res, std::move(async), data);
		} {
			// second read command should be failed with timeout error due to queue timeout
			ELLIPTICS_REQUIRE_ERROR(res, std::move(async_timeouted), -ETIMEDOUT);
		} {
			// there should be no aftereffect, so next read request should be succeeded
			ELLIPTICS_COMPARE_REQUIRE(res, s.read_data(key, 0, 0), data);
		}
	}

}

static bool register_tests(const nodes_data *setup) {
	auto n = setup->node->get_native();

	ELLIPTICS_TEST_CASE(test_queue_timeout, use_session(n, { group }, 0, 0), setup);
	ELLIPTICS_TEST_CASE(test_queue_ack_timeout, use_session(n, { group }, 0, 0), setup);
	ELLIPTICS_TEST_CASE(test_overridden_queue_timeout,
	                    use_session(n, {group_with_overridden_queue_timeout}, 0, 0),
	                    setup);

	return true;
}

nodes_data::ptr configure_test_setup_from_args(int argc, char *argv[]) {
	namespace bpo = boost::program_options;

	bpo::variables_map vm;
	bpo::options_description generic("Test options");

	std::string path;

	generic.add_options()
		("help", "This help message")
		("path", bpo::value(&path), "Path where to store everything")
		;

	bpo::store(bpo::parse_command_line(argc, argv, generic), vm);
	bpo::notify(vm);

	if (vm.count("help")) {
		std::cerr << generic;
		return nullptr;
	}

	return configure_test_setup(path);
}

} /* namespace tests */


/*
 * Common test initialization routine.
 */
using namespace tests;
using namespace boost::unit_test;

/*FIXME: forced to use global variable and plain function wrapper
 * because of the way how init_test_main works in boost.test,
 * introducing a global fixture would be a proper way to handle
 * global test setup
 */
namespace {

std::shared_ptr<nodes_data> setup;

bool init_func()
{
	return register_tests(setup.get());
}

}

int main(int argc, char *argv[])
{
	srand(time(nullptr));

	// we own our test setup
	setup = configure_test_setup_from_args(argc, argv);

	int result = unit_test_main(init_func, argc, argv);

	// disassemble setup explicitly, to be sure about where its lifetime ends
	setup.reset();

	return result;
}

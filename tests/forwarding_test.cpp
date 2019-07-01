#include <boost/program_options.hpp>

#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include "test_base.hpp"

namespace tests {
namespace bu = boost::unit_test;

nodes_data::ptr configure_test_setup(const std::string &path) {
	auto server_config = [](const config_data &c) {
		auto server = server_config::default_value();
		return server.apply_options(c);
	};

	auto configs = {server_config(config_data()("group", 1)("wait_timeout", 3)),
	                server_config(config_data()("group", 2)("wait_timeout", 3)),
	                server_config(config_data()("group", 3)("wait_timeout", 1))};

	start_nodes_config config{bu::results_reporter::get_stream(), configs, path};
	config.fork = true;

	return start_nodes(config);
}

void test_forward_lookup(ioremap::elliptics::newapi::session &session, const nodes_data *setup) {
	auto s = session.clone();
	s.set_groups({1, 2, 3});
	s.set_filter(ioremap::elliptics::filters::all_final);

	const auto forward = setup->nodes.front().remote();
	s.set_forward(forward);

	{
		// Value mustn't be found
		auto async = s.lookup({"some_key"});

		size_t count = 0;
		for (const auto &result: async) {
			BOOST_REQUIRE_EQUAL(result.status(), -ENOENT);
			std::string forward_address = "";
			BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());
			++count;
		}
		BOOST_REQUIRE_EQUAL(count, 3);
	}

	{
		dnet_id id;
		s.transform("some_key", id);
		id.group_id = 2;
		std::string json = R"json({"key": "data"})json";
		std::string data("data");

		s.write(id, json, 0, data, 0).wait();

		// Value must be found since we've just written it
		auto async = s.lookup({"some_key"});

		size_t count = 0;
		for (const auto &result: async) {
			BOOST_REQUIRE_EQUAL(result.status(), 0);
			std::string forward_address = "";
			BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());

			auto record_info = result.record_info();
			BOOST_REQUIRE_EQUAL(record_info.json_size, json.size());
			BOOST_REQUIRE_EQUAL(record_info.data_size, data.size());

			++count;
		}
		BOOST_REQUIRE_EQUAL(count, 1);
	}
}

void test_forward_lookup_2_nothing(ioremap::elliptics::newapi::session &session) {
	auto s = session.clone();
	s.set_groups({1, 2, 3});
	s.set_filter(ioremap::elliptics::filters::all_final);

	s.set_forward(ioremap::elliptics::address());

	auto async = s.lookup({"nonexistent key"});

	size_t count = 0;
	for (const auto &result: async) {
		BOOST_REQUIRE_EQUAL(result.status(), -ENXIO);
		++count;
	}
	BOOST_REQUIRE_EQUAL(count, 3);
}

void test_forward_lookup_2_nonexistent_groups(ioremap::elliptics::newapi::session &session, const nodes_data *setup) {
	auto s = session.clone();
	s.set_groups({5, 6, 7});
	s.set_filter(ioremap::elliptics::filters::all_final);

	const auto forward = setup->nodes.front().remote();
	s.set_forward(forward);

	auto async = s.lookup({"nonexistent key"});

	size_t count = 0;
	for (const auto &result: async) {
		BOOST_REQUIRE_EQUAL(result.status(), -ENOTSUP);
		BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());
		++count;
	}
	BOOST_REQUIRE_EQUAL(count, 3);
}

void test_forward_read_with_deadline(ioremap::elliptics::newapi::session &session, const nodes_data *setup) {
	auto s = session.clone();
	s.set_groups({2});
	s.set_filter(ioremap::elliptics::filters::all_final);

	const auto delayed_remote = setup->nodes[1].remote();
	static const auto delayed_backend = 0;
	s.set_delay(delayed_remote, delayed_backend, 3000).wait();

	s.set_timeout(1);
	const auto forward = setup->nodes.front().remote();
	s.set_forward(forward);

	{
		auto async = s.lookup({"nonexistent key"});
		size_t count = 0;
		for (const auto &result: async) {
			BOOST_REQUIRE_EQUAL(result.status(), -ETIMEDOUT);
			BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());
			++count;
		}
		BOOST_REQUIRE_EQUAL(count, 1);
	}

	{
		auto async = s.read_json({"nonexistent key"});
		size_t count = 0;
		for (const auto &result: async) {
			BOOST_REQUIRE_EQUAL(result.status(), -ETIMEDOUT);
			BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());
			++count;
		}
		BOOST_REQUIRE_EQUAL(count, 1);
	}

	{
		auto async = s.write({"test_forward_read_with_deadline: some key"}, "", 0, "some data", 0);
		size_t count = 0;
		for (const auto &result: async) {
			BOOST_REQUIRE_EQUAL(result.status(), -ETIMEDOUT);
			BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());
			++count;
		}
		BOOST_REQUIRE_EQUAL(count, 1);
	}

	// reset backend's delay to 0
	s.set_delay(delayed_remote, delayed_backend, 0).wait();
}

void test_forward_lookup_with_deadline_at_proxy(ioremap::elliptics::newapi::session &session, const nodes_data *setup) {
	auto s = session.clone();
	s.set_groups({2});
	s.set_filter(ioremap::elliptics::filters::all_final);

	const auto delayed_remote = setup->nodes[1].remote();
	static const auto delayed_backend = 0;
	s.set_delay(delayed_remote, delayed_backend, 2000).wait();

	// Doesn't matter. Response with ETIMEDOUT will be received earlier, timeout will be triggered in forward node.
	s.set_timeout(50);

	const auto forward = setup->nodes[2].remote();
	s.set_forward(forward);

	{
		auto async = s.lookup({"nonexistent key"});
		size_t count = 0;
		for (const auto &result: async) {
			BOOST_REQUIRE_EQUAL(result.status(), -ETIMEDOUT);
			BOOST_REQUIRE_EQUAL(dnet_addr_string(result.address()), forward.to_string());
			++count;
		}
		BOOST_REQUIRE_EQUAL(count, 1);
	}

	// reset backend's delay to 0
	s.set_delay(delayed_remote, delayed_backend, 0).wait();
}

bool register_tests(const nodes_data *setup)
{
	auto n = setup->node->get_native();

	ELLIPTICS_TEST_CASE(test_forward_lookup, use_session(n), setup);
	ELLIPTICS_TEST_CASE(test_forward_lookup_2_nothing, use_session(n));
	ELLIPTICS_TEST_CASE(test_forward_lookup_2_nonexistent_groups, use_session(n), setup);
	ELLIPTICS_TEST_CASE(test_forward_read_with_deadline, use_session(n), setup);
	ELLIPTICS_TEST_CASE(test_forward_lookup_with_deadline_at_proxy, use_session(n), setup);

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

	return tests::configure_test_setup(path);
}

} /* namespace tests */

namespace {
std::shared_ptr<tests::nodes_data> setup;

bool init_func() {
	return tests::register_tests(setup.get());
}

} /* namespace */

int main(int argc, char *argv[]) {
	srand(time(nullptr));

	setup = tests::configure_test_setup_from_args(argc, argv);
	int result = boost::unit_test::unit_test_main(init_func, argc, argv);
	setup.reset();

	return result;
}

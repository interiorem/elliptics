#pragma once

namespace ioremap { namespace grpc {

class job_t {
public:
	virtual void proceed(bool ok) = 0;
	virtual ~job_t() = default;
};

}} // namespace ioremap::grpc

#pragma once

namespace ioremap { namespace grpc {

class job_t {
/*
 * Abstract interface for asynchronous work by grpc::CompletionQueue.
 * Completion queue signals events from the network connection and each signal
 * CompletionQueue::Next(bool) with the ok flag invoke the proceed(bool) method.
 * Meanings of the flag can be different for applications.
 */

public:
	virtual void proceed(bool ok) = 0;
	virtual ~job_t() = default;
};

}} // namespace ioremap::grpc

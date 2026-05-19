/*
 * Copyright (c) 2026 Chad Attermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#pragma once

#include <stdint.h>

namespace microStore {

// I/O failure callback (ur-patches).
//
// Some embedded consumers route file paths through microStore that
// actually back onto a hot-swappable mount point — most obviously
// `/sd/...` paths on devices with a microSD card. When the card is
// pulled mid-session, microStore's POSIX-style open/read/write/etc.
// just return failure values; the consumer's hardware-presence
// bookkeeping (which microStore knows nothing about) has no way to
// learn about it until something independently re-probes the
// hardware. The failure callback closes that gap without dragging
// hardware concerns into the library: consumers register a single
// function pointer that microStore invokes on every failed I/O,
// passing the path that triggered it and a tag for the operation
// kind. The consumer's callback decides whether (and how) to react
// — e.g. firmware can run an SD presence probe for `/sd/*` paths
// and ignore everything else.
//
// Set the callback once at program start with set_io_failure_callback().
// Pass nullptr to clear. Storage is a function-local static so this
// is safe to use from header-only code without ODR issues.

enum class IoOp : uint8_t {
	Open,        // FileSystem::open() returned an invalid File
	Read,        // File::read returned 0 / -1 unexpectedly
	Write,       // File::write returned short
	Seek,        // File::seek returned <0
	Exists,      // FileSystem::exists returned false
	Remove,      // FileSystem::remove returned false
	Rename,      // FileSystem::rename returned false
	Mkdir,       // FileSystem::mkdir returned false
	Rmdir,       // FileSystem::rmdir returned false
	ReadFile,    // FileSystem::readFile returned 0 unexpectedly
	WriteFile,   // FileSystem::writeFile returned 0 / short
};

using IoFailureCallback = void (*)(const char* path, IoOp op);

namespace _io_failure {
	inline IoFailureCallback& current() {
		static IoFailureCallback cb = nullptr;
		return cb;
	}
}

inline void set_io_failure_callback(IoFailureCallback cb) {
	_io_failure::current() = cb;
}

inline void _notify_io_failure(const char* path, IoOp op) {
	IoFailureCallback cb = _io_failure::current();
	if (cb) cb(path ? path : "", op);
}

}

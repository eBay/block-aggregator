/************************************************************************
Copyright 2021, eBay, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include <Aggregator/LoaderOutputStreamLogging.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>

namespace nuclm {

LoaderBlockOutputStream::LoaderBlockOutputStream(DB::ContextMutablePtr context_) :
        ostr{}, out_buf(ostr), block_out_stream{nullptr}, context(context_) {}

void LoaderBlockOutputStream::initBlockOutputStream(const DB::Block& block) {
    static std::string current_format = "TabSeparated";
    if (!block_out_stream) {
        block_out_stream = context->getOutputStream(current_format, out_buf, block);
        block_out_stream->writePrefix();
    }
}

void LoaderBlockOutputStream::write(const DB::Block& block) { block_out_stream->write(block); }

// NOTE: do I need to reset the string buffer after the flush, or the flush will perform the clean up?
std::string LoaderBlockOutputStream::flush() {
    block_out_stream->flush();
    std::string flushed_result = ostr.str();

    // empty the current string buffer
    std::ostringstream empty_stream;
    ostr.swap(empty_stream);

    return flushed_result;
}

void LoaderBlockOutputStream::writePrefix() { block_out_stream->writePrefix(); }

void LoaderBlockOutputStream::writeSuffix() { block_out_stream->writeSuffix(); }

// set totals, delegate to the underlying block output stream
void LoaderBlockOutputStream::setTotals(const DB::Block& block) { block_out_stream->setTotals(block); }

// set extremes, delegate to the underlying block output stream
void LoaderBlockOutputStream::setExtremes(const DB::Block& block) { block_out_stream->setExtremes(block); }

// on progress, delegate to the underlying block output stream
void LoaderBlockOutputStream::onProgress(const DB::Progress& value) { block_out_stream->onProgress(value); }

LoaderLogOutputStream::LoaderLogOutputStream() : ostr{}, out_buf(ostr), logs_out_stream{nullptr} {}

void LoaderLogOutputStream::initLogOutputStream() {
    if (!logs_out_stream) {
        logs_out_stream = std::make_shared<DB::InternalTextLogsRowOutputStream>(out_buf, false);
        logs_out_stream->writePrefix();
    }
};

void LoaderLogOutputStream::write(const DB::Block& block) { logs_out_stream->write(block); }

std::string LoaderLogOutputStream::flush() {
    logs_out_stream->flush();
    std::string flushed_result = ostr.str();

    // empty the current string buffer
    std::ostringstream empty_stream;
    ostr.swap(empty_stream);

    return flushed_result;
}

void LoaderLogOutputStream::writePrefix() { logs_out_stream->writePrefix(); }

void LoaderLogOutputStream::writeSuffix() { logs_out_stream->writeSuffix(); }

} // namespace nuclm

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

#pragma once

#include <Interpreters/Context.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <IO/WriteBufferFromOStream.h>

#include <sstream>

namespace nuclm {

class LoaderBlockOutputStream {
  public:
    LoaderBlockOutputStream(DB::ContextMutablePtr context);
    ~LoaderBlockOutputStream() = default;

    void initBlockOutputStream(const DB::Block& block);

    void write(const DB::Block& block);

    // return the block output stream information as a string and reset the stream.
    std::string flush();

    // set totals, delegate to the underlying block output stream
    void setTotals(const DB::Block& block);

    // set extremes, delegate to the underlying block output stream
    void setExtremes(const DB::Block& block);

    // on progress, delegate to the underlying block output stream
    void onProgress(const DB::Progress& value);

    // writeSuffix, delegate to the underlying block output stream
    void writeSuffix();

    // writePrefix, delegate to the underlying block output stream
    void writePrefix();

  private:
    std::ostringstream ostr; // for logging purpose
    DB::WriteBufferFromOStream out_buf;
    DB::BlockOutputStreamPtr block_out_stream;
    DB::ContextMutablePtr context;
};

class LoaderLogOutputStream {
  public:
    LoaderLogOutputStream();
    ~LoaderLogOutputStream() = default;

    void initLogOutputStream();

    void write(const DB::Block& block);

    // return the block output stream information as a string and reset the stream.
    std::string flush();

    // writeSuffix, delegate to the underlying block output stream
    void writeSuffix();

    // writePrefix, delegate to the underlying block output stream
    void writePrefix();

  private:
    std::ostringstream ostr; // for logging purpose
    DB::WriteBufferFromOStream out_buf;
    DB::BlockOutputStreamPtr logs_out_stream;
};

using LoaderBlockOutputStreamPtr = std::shared_ptr<LoaderBlockOutputStream>;
using LoaderLogOutputStreamPtr = std::shared_ptr<LoaderLogOutputStream>;

}; // namespace nuclm

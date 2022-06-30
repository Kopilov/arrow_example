#pragma once

#include <arrow/builder.h>

namespace dataframe_example {

/*
 * Generate String (ASCII only) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::StringBuilder>> getAsciiStringSequenceBuilder(long size);

/*
 * Generate String (UTF-8) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::StringBuilder>> getUtf8StringSequenceBuilder(long size);

/*
 * Generate LargeString sequence builder
 */
arrow::Result<std::shared_ptr<arrow::LargeStringBuilder>> getLargeStringSequenceBuilder(long size);

/*
 * Generate Int64 (Long) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Int64Builder>> getInt64SequenceBuilder(long size);

} //end dataframe_example

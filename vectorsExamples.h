#pragma once

#include <arrow/builder.h>

namespace dataframe_example {

/*
 * Generate String (ASCII only) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::StringBuilder>> getAsciiStringSequenceBuilder(long size, bool withNulls);

/*
 * Generate String (UTF-8) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::StringBuilder>> getUtf8StringSequenceBuilder(long size, bool withNulls);

/*
 * Generate LargeString sequence builder
 */
arrow::Result<std::shared_ptr<arrow::LargeStringBuilder>> getLargeStringSequenceBuilder(long size, bool withNulls);

/*
 * Generate Boolean sequence builder
 */
arrow::Result<std::shared_ptr<arrow::BooleanBuilder>> getBooleanSequenceBuilder(long size, bool withNulls);

/*
 * Generate Int8 (Byte) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Int8Builder>> getInt8SequenceBuilder(long size, bool withNulls);

/*
 * Generate Int16 (Short) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Int16Builder>> getInt16SequenceBuilder(long size, bool withNulls);

/*
 * Generate Int32 (Int) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Int32Builder>> getInt32SequenceBuilder(long size, bool withNulls);

/*
 * Generate Int64 (Long) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Int64Builder>> getInt64SequenceBuilder(long size, bool withNulls);

/*
 * Generate UInt8 sequence builder
 */
arrow::Result<std::shared_ptr<arrow::UInt8Builder>> getUInt8SequenceBuilder(long size, bool withNulls);

/*
 * Generate UInt16 sequence builder
 */
arrow::Result<std::shared_ptr<arrow::UInt16Builder>> getUInt16SequenceBuilder(long size, bool withNulls);

/*
 * Generate UInt32 sequence builder
 */
arrow::Result<std::shared_ptr<arrow::UInt32Builder>> getUInt32SequenceBuilder(long size, bool withNulls);

/*
 * Generate UInt64 sequence builder
 */
arrow::Result<std::shared_ptr<arrow::UInt64Builder>> getUInt64SequenceBuilder(long size, bool withNulls);


/*
 * Generate HalfFloat sequence builder
 */
arrow::Result<std::shared_ptr<arrow::HalfFloatBuilder>> getHalfFloatSequenceBuilder(long size, bool withNulls);

/*
 * Generate Float sequence builder
 */
arrow::Result<std::shared_ptr<arrow::FloatBuilder>> getFloatSequenceBuilder(long size, bool withNulls);

/*
 * Generate Double sequence builder
 */
arrow::Result<std::shared_ptr<arrow::DoubleBuilder>> getDoubleSequenceBuilder(long size, bool withNulls);


/*
 * Generate Date32 (a number of days since UNIX epoch) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Date32Builder>> getDate32SequenceBuilder(long size, bool withNulls);

/*
 * Generate Date64 (a number of milliseconds since UNIX epoch) sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Date64Builder>> getDate64SequenceBuilder(long size, bool withNulls);

/*
 * Generate Time32<TimeUnit> sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Time32Builder>> getTime32SequenceBuilder(long size, bool withNulls, arrow::TimeUnit::type unit);

/*
 * Generate Time64<TimeUnit> sequence builder
 */
arrow::Result<std::shared_ptr<arrow::Time64Builder>> getTime64SequenceBuilder(long size, bool withNulls, arrow::TimeUnit::type unit);
    
} //end dataframe_example

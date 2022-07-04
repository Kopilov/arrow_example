#include <sstream>
#include <cmath>
#include <arrow/result.h>
#include "vectorsExamples.h"

namespace dataframe_example {

arrow::Result<std::shared_ptr<arrow::StringBuilder>> getAsciiStringSequenceBuilder(long size) {
    std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
    
    for (long i = 0; i < size; i++) {
        std::stringstream sstm;
        sstm << "Test Example " << i;
        ARROW_RETURN_NOT_OK(builder->Append(sstm.str()));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::StringBuilder>> getUtf8StringSequenceBuilder(long size) {
    std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
    
    for (long i = 0; i < size; i++) {
        std::stringstream sstm;
        sstm << "Тестовый пример " << i;
        ARROW_RETURN_NOT_OK(builder->Append(sstm.str()));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::LargeStringBuilder>> getLargeStringSequenceBuilder(long size) {
    std::shared_ptr<arrow::LargeStringBuilder> builder = std::make_shared<arrow::LargeStringBuilder>();

    for (long i = 0; i < size; i++) {
        std::stringstream sstm;
        sstm << "Test Example Should Be Large " << i;
        ARROW_RETURN_NOT_OK(builder->Append(sstm.str()));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::BooleanBuilder>> getBooleanSequenceBuilder(long size) {
    std::shared_ptr<arrow::BooleanBuilder> builder = std::make_shared<arrow::BooleanBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i % 2 == 0));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int8Builder>> getInt8SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int8Builder> builder = std::make_shared<arrow::Int8Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 10));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int16Builder>> getInt16SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int16Builder> builder = std::make_shared<arrow::Int16Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 1000));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int32Builder>> getInt32SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int32Builder> builder = std::make_shared<arrow::Int32Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 100000000L));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int64Builder>> getInt64SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 100000000000000000L));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt8Builder>> getUInt8SequenceBuilder(long size) {
    std::shared_ptr<arrow::UInt8Builder> builder = std::make_shared<arrow::UInt8Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 10));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt16Builder>> getUInt16SequenceBuilder(long size) {
    std::shared_ptr<arrow::UInt16Builder> builder = std::make_shared<arrow::UInt16Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 1000));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt32Builder>> getUInt32SequenceBuilder(long size) {
    std::shared_ptr<arrow::UInt32Builder> builder = std::make_shared<arrow::UInt32Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 100000000L));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt64Builder>> getUInt64SequenceBuilder(long size) {
    std::shared_ptr<arrow::UInt64Builder> builder = std::make_shared<arrow::UInt64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 100000000000000000L));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::HalfFloatBuilder>> getHalfFloatSequenceBuilder(long size) {
    std::shared_ptr<arrow::HalfFloatBuilder> builder = std::make_shared<arrow::HalfFloatBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(std::pow(2.0, (float)i)));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::FloatBuilder>> getFloatSequenceBuilder(long size) {
    std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(std::pow(2.0, (double)i)));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::DoubleBuilder>> getDoubleSequenceBuilder(long size) {
    std::shared_ptr<arrow::DoubleBuilder> builder = std::make_shared<arrow::DoubleBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(std::pow(2.0, (double)i)));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Date32Builder>> getDate32SequenceBuilder(long size) {
    std::shared_ptr<arrow::Date32Builder> builder = std::make_shared<arrow::Date32Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 30));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Date64Builder>> getDate64SequenceBuilder(long size) {
    std::shared_ptr<arrow::Date64Builder> builder = std::make_shared<arrow::Date64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i * 1000 * 60 * 60 * 24 * 30));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Time32Builder>> getTime32SequenceBuilder(long size, arrow::TimeUnit::type unit) {
    std::shared_ptr<arrow::Time32Builder> builder = std::make_shared<arrow::Time32Builder>(arrow::time32(unit), arrow::default_memory_pool());

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Time64Builder>> getTime64SequenceBuilder(long size, arrow::TimeUnit::type unit) {
    std::shared_ptr<arrow::Time64Builder> builder = std::make_shared<arrow::Time64Builder>(arrow::time64(unit), arrow::default_memory_pool());

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i));
    }
    return builder;
}

} //end dataframe_example

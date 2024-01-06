#include <sstream>
#include <cmath>
#include <arrow/result.h>
#include "vectorsExamples.h"

namespace dataframe_example {

/**
 * Return true if row with given number should contain null values
 */
bool saveNull(long rowNumber) {
    return (rowNumber + 1) % 5 == 0;
}

arrow::Status appendValueOrNull(bool withNulls, long rowNumber, std::function<arrow::Status()> appendValue, std::function<arrow::Status()> appendNull) {
    if (withNulls && saveNull(rowNumber)) {
        ARROW_RETURN_NOT_OK(appendNull());
    } else {
        ARROW_RETURN_NOT_OK(appendValue());
    }
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::StringBuilder>> getAsciiStringSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
    
    for (long i = 0; i < size; i++) {
        std::shared_ptr<std::stringstream> sstm = std::make_shared<std::stringstream>();
        *sstm << "Test Example " << i;
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, sstm](){return builder->Append(sstm->str());},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::StringBuilder>> getUtf8StringSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
    
    for (long i = 0; i < size; i++) {
        std::shared_ptr<std::stringstream> sstm = std::make_shared<std::stringstream>();
        *sstm << "Тестовый пример " << i;
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, sstm](){return builder->Append(sstm->str());},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::LargeStringBuilder>> getLargeStringSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::LargeStringBuilder> builder = std::make_shared<arrow::LargeStringBuilder>();

    for (long i = 0; i < size; i++) {
        std::shared_ptr<std::stringstream> sstm = std::make_shared<std::stringstream>();
        *sstm << "Test Example Should Be Large " << i;
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, sstm](){return builder->Append(sstm->str());},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::BooleanBuilder>> getBooleanSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::BooleanBuilder> builder = std::make_shared<arrow::BooleanBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i % 2 == 0);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int8Builder>> getInt8SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::Int8Builder> builder = std::make_shared<arrow::Int8Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 10);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int16Builder>> getInt16SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::Int16Builder> builder = std::make_shared<arrow::Int16Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 1000);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int32Builder>> getInt32SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::Int32Builder> builder = std::make_shared<arrow::Int32Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 100000000);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Int64Builder>> getInt64SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 100000000000000000L);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt8Builder>> getUInt8SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::UInt8Builder> builder = std::make_shared<arrow::UInt8Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 10);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt16Builder>> getUInt16SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::UInt16Builder> builder = std::make_shared<arrow::UInt16Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 1000);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt32Builder>> getUInt32SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::UInt32Builder> builder = std::make_shared<arrow::UInt32Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 100000000);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::UInt64Builder>> getUInt64SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::UInt64Builder> builder = std::make_shared<arrow::UInt64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 100000000000000000L);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::HalfFloatBuilder>> getHalfFloatSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::HalfFloatBuilder> builder = std::make_shared<arrow::HalfFloatBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(std::pow(2.0, (float)i));},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::FloatBuilder>> getFloatSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(std::pow(2.0, (float)i));},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::DoubleBuilder>> getDoubleSequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::DoubleBuilder> builder = std::make_shared<arrow::DoubleBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(std::pow(2.0, (double)i));},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Date32Builder>> getDate32SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::Date32Builder> builder = std::make_shared<arrow::Date32Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 30);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Date64Builder>> getDate64SequenceBuilder(long size, bool withNulls) {
    std::shared_ptr<arrow::Date64Builder> builder = std::make_shared<arrow::Date64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i * 1000 * 60 * 60 * 24 * 30);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Time32Builder>> getTime32SequenceBuilder(long size, bool withNulls, arrow::TimeUnit::type unit) {
    std::shared_ptr<arrow::Time32Builder> builder = std::make_shared<arrow::Time32Builder>(arrow::time32(unit), arrow::default_memory_pool());

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

arrow::Result<std::shared_ptr<arrow::Time64Builder>> getTime64SequenceBuilder(long size, bool withNulls, arrow::TimeUnit::type unit) {
    std::shared_ptr<arrow::Time64Builder> builder = std::make_shared<arrow::Time64Builder>(arrow::time64(unit), arrow::default_memory_pool());

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(appendValueOrNull(withNulls, i,
                [builder, i](){return builder->Append(i);},
                [builder](){return builder->AppendNull();}
        ));
    }
    return builder;
}

/*
 * Generate Null sequence builder
 */
arrow::Result<std::shared_ptr<arrow::NullBuilder>> getNullSequenceBuilder(long size) {
    std::shared_ptr<arrow::NullBuilder> builder = std::make_shared<arrow::NullBuilder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
    }
    return builder;
}

} //end dataframe_example

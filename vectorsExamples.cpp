#include <sstream>
#include <arrow/result.h>
#include "vectorsExamples.h"

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

arrow::Result<std::shared_ptr<arrow::Int64Builder>> getInt64SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>();

    for (long i = 0; i < size; i++) {
        ARROW_RETURN_NOT_OK(builder->Append(i));
    }
    return builder;
}

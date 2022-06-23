#include <sstream>
#include "vectorsExamples.h"

std::shared_ptr<arrow::StringBuilder> getAsciiStringSequenceBuilder(long size) {
    std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
    
    for (long i = 0; i < size; i++) {
        std::stringstream sstm;
        sstm << "Test Example " << i;
        builder->Append(sstm.str());
    }
    return builder;
}

std::shared_ptr<arrow::StringBuilder> getUtf8StringSequenceBuilder(long size) {
    std::shared_ptr<arrow::StringBuilder> builder = std::make_shared<arrow::StringBuilder>();
    
    for (long i = 0; i < size; i++) {
        std::stringstream sstm;
        sstm << "Тестовый пример " << i;
        builder->Append(sstm.str());
    }
    return builder;
}

std::shared_ptr<arrow::Int64Builder> getInt64SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>();

    for (long i = 0; i < size; i++) {
        builder->Append(i);
    }
    return builder;
}

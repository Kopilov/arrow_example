#include <iostream>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "vectorsExamples.h"


namespace dataframe_example {

void addExample(
    std::string name,
    std::function<arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>()> prepareExampleBuilder,
    std::vector<std::shared_ptr<arrow::Field>> &fieldsExamples,
    std::vector<std::shared_ptr<arrow::Array>> &dataExamples
) {
    std::shared_ptr<arrow::ArrayBuilder> builder = prepareExampleBuilder().ValueOrDie();
    fieldsExamples.push_back(field(name, builder->type()));
    dataExamples.push_back(builder->Finish().ValueOrDie());
}

std::shared_ptr<arrow::RecordBatch> createDemoRecordBatch(long size) {

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> data;

    addExample("asciiString", [size](){return getAsciiStringSequenceBuilder(size);}, fields, data);
    addExample("utf8String", [size](){return getUtf8StringSequenceBuilder(size);}, fields, data);
    addExample("largeString", [size](){return getLargeStringSequenceBuilder(size);}, fields, data);

    addExample("boolean", [size](){return getBooleanSequenceBuilder(size);}, fields, data);

    addExample("byte", [size](){return getInt8SequenceBuilder(size);}, fields, data);
    addExample("short", [size](){return getInt16SequenceBuilder(size);}, fields, data);
    addExample("int", [size](){return getInt32SequenceBuilder(size);}, fields, data);
    addExample("longInt", [size](){return getInt64SequenceBuilder(size);}, fields, data);

    addExample("unsigned_byte", [size](){return getUInt8SequenceBuilder(size);}, fields, data);
    addExample("unsigned_short", [size](){return getUInt16SequenceBuilder(size);}, fields, data);
    addExample("unsigned_int", [size](){return getUInt32SequenceBuilder(size);}, fields, data);
    addExample("unsigned_longInt", [size](){return getUInt64SequenceBuilder(size);}, fields, data);

    addExample("halfFloat", [size](){return getHalfFloatSequenceBuilder(size);}, fields, data);
    addExample("float", [size](){return getFloatSequenceBuilder(size);}, fields, data);
    addExample("double", [size](){return getDoubleSequenceBuilder(size);}, fields, data);

    addExample("date32", [size](){return getDate32SequenceBuilder(size);}, fields, data);
    addExample("date64", [size](){return getDate64SequenceBuilder(size);}, fields, data);

    addExample("time32_seconds", [size](){return getTime32SequenceBuilder(size, arrow::TimeUnit::SECOND);}, fields, data);
    addExample("time32_milli", [size](){return getTime32SequenceBuilder(size, arrow::TimeUnit::MILLI);}, fields, data);

    addExample("time64_micro", [size](){return getTime64SequenceBuilder(size, arrow::TimeUnit::MICRO);}, fields, data);
    addExample("time64_nano", [size](){return getTime64SequenceBuilder(size, arrow::TimeUnit::NANO);}, fields, data);

    std::shared_ptr<arrow::RecordBatch> recordBatch = arrow::RecordBatch::Make(arrow::schema(fields), size, data);
    return recordBatch;
}

arrow::Status writeAllExamples() {
    std::shared_ptr<arrow::RecordBatch> recordBatch1 = createDemoRecordBatch(100);
    std::shared_ptr<arrow::RecordBatch> recordBatch2 = createDemoRecordBatch(200);

    std::shared_ptr<arrow::io::FileOutputStream> randomAccessOut = arrow::io::FileOutputStream::Open("testFeather.arrow").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> randomAccessWriter = arrow::ipc::MakeFileWriter(randomAccessOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(randomAccessWriter->Close());

    std::shared_ptr<arrow::io::FileOutputStream> ipcStreamOut = arrow::io::FileOutputStream::Open("testIPC.arrow").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipcStreamWriter = arrow::ipc::MakeStreamWriter(ipcStreamOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->Close());

    return arrow::Status::OK();
}

} //end dataframe_example

int main(int argc, char* argv[]) {
    arrow::Status status = dataframe_example::writeAllExamples();
    if (status == arrow::Status::OK()) {
        return 0;
    } else {
        return 1;
    }
}

#include <iostream>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "vectorsExamples.h"


namespace dataframe_example {

void addExample(
    std::string name,
    long size,
    std::function<arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>(long)> prepareExampleBuilder,
    std::vector<std::shared_ptr<arrow::Field>> &fieldsExamples,
    std::vector<std::shared_ptr<arrow::Array>> &dataExamples
) {
    std::shared_ptr<arrow::ArrayBuilder> builder = prepareExampleBuilder(size).ValueOrDie();
    fieldsExamples.push_back(field(name, builder->type()));
    dataExamples.push_back(builder->Finish().ValueOrDie());
}

std::shared_ptr<arrow::RecordBatch> createDemoRecordBatch() {
    long size = 100;

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> data;

    addExample("asciiString", size, getAsciiStringSequenceBuilder, fields, data);
    addExample("utf8String", size, getUtf8StringSequenceBuilder, fields, data);
    addExample("largeString", size, getLargeStringSequenceBuilder, fields, data);

    addExample("boolean", size, getBooleanSequenceBuilder, fields, data);

    addExample("byte", size, getInt8SequenceBuilder, fields, data);
    addExample("short", size, getInt16SequenceBuilder, fields, data);
    addExample("int", size, getInt32SequenceBuilder, fields, data);
    addExample("longInt", size, getInt64SequenceBuilder, fields, data);

    addExample("unsigned_byte", size, getUInt8SequenceBuilder, fields, data);
    addExample("unsigned_short", size, getUInt16SequenceBuilder, fields, data);
    addExample("unsigned_int", size, getUInt32SequenceBuilder, fields, data);
    addExample("unsigned_longInt", size, getUInt64SequenceBuilder, fields, data);

    std::shared_ptr<arrow::RecordBatch> recordBatch = arrow::RecordBatch::Make(arrow::schema(fields), size, data);
    return recordBatch;
}

void writeAndClose(std::shared_ptr<arrow::ipc::RecordBatchWriter> writer, std::shared_ptr<arrow::RecordBatch> recordBatch) {
    arrow::Status s = writer->WriteRecordBatch(*recordBatch);
    s = writer->Close();

}

} //end dataframe_example

int main(int argc, char* argv[]) {
    std::shared_ptr<arrow::RecordBatch> recordBatch = dataframe_example::createDemoRecordBatch();

    std::shared_ptr<arrow::io::FileOutputStream> randomAccessOut = arrow::io::FileOutputStream::Open("testFeather.arrow").ValueOrDie();
    dataframe_example::writeAndClose(arrow::ipc::MakeFileWriter(randomAccessOut, recordBatch->schema()).ValueOrDie(), recordBatch);

    std::shared_ptr<arrow::io::FileOutputStream> ipcStreamOut = arrow::io::FileOutputStream::Open("testIPC.arrow").ValueOrDie();
    dataframe_example::writeAndClose(arrow::ipc::MakeStreamWriter(ipcStreamOut, recordBatch->schema()).ValueOrDie(), recordBatch);

    return 0;
}

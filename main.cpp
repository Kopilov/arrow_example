#include <iostream>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "vectorsExamples.h"


namespace dataframe_example {

void addExample(
    std::string name,
    std::function<arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>()> prepareExampleBuilder,
    bool nullable,
    std::vector<std::shared_ptr<arrow::Field>> &fieldsExamples,
    std::vector<std::shared_ptr<arrow::Array>> &dataExamples
) {
    std::shared_ptr<arrow::ArrayBuilder> builder = prepareExampleBuilder().ValueOrDie();
    fieldsExamples.push_back(field(name, builder->type(), nullable));
    dataExamples.push_back(builder->Finish().ValueOrDie());
}

std::shared_ptr<arrow::RecordBatch> createDemoRecordBatch(long size, bool nullable, bool withNulls) {

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> data;

    addExample("asciiString", [size, withNulls](){return getAsciiStringSequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("utf8String", [size, withNulls](){return getUtf8StringSequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("largeString", [size, withNulls](){return getLargeStringSequenceBuilder(size, withNulls);}, nullable, fields, data);

    addExample("boolean", [size, withNulls](){return getBooleanSequenceBuilder(size, withNulls);}, nullable, fields, data);

    addExample("byte", [size, withNulls](){return getInt8SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("short", [size, withNulls](){return getInt16SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("int", [size, withNulls](){return getInt32SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("longInt", [size, withNulls](){return getInt64SequenceBuilder(size, withNulls);}, nullable, fields, data);

    addExample("unsigned_byte", [size, withNulls](){return getUInt8SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("unsigned_short", [size, withNulls](){return getUInt16SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("unsigned_int", [size, withNulls](){return getUInt32SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("unsigned_longInt", [size, withNulls](){return getUInt64SequenceBuilder(size, withNulls);}, nullable, fields, data);

    //addExample("halfFloat", [size, withNulls](){return getHalfFloatSequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("float", [size, withNulls](){return getFloatSequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("double", [size, withNulls](){return getDoubleSequenceBuilder(size, withNulls);}, nullable, fields, data);

    addExample("date32", [size, withNulls](){return getDate32SequenceBuilder(size, withNulls);}, nullable, fields, data);
    addExample("date64", [size, withNulls](){return getDate64SequenceBuilder(size, withNulls);}, nullable, fields, data);

    addExample("time32_seconds", [size, withNulls](){return getTime32SequenceBuilder(size, withNulls, arrow::TimeUnit::SECOND);}, nullable, fields, data);
    addExample("time32_milli", [size, withNulls](){return getTime32SequenceBuilder(size, withNulls, arrow::TimeUnit::MILLI);}, nullable, fields, data);

    addExample("time64_micro", [size, withNulls](){return getTime64SequenceBuilder(size, withNulls, arrow::TimeUnit::MICRO);}, nullable, fields, data);
    addExample("time64_nano", [size, withNulls](){return getTime64SequenceBuilder(size, withNulls, arrow::TimeUnit::NANO);}, nullable, fields, data);

    std::shared_ptr<arrow::RecordBatch> recordBatch = arrow::RecordBatch::Make(arrow::schema(fields), size, data);
    return recordBatch;
}


/**
 * Create Feather and IPC files with two batches containing nullable (this is default) columns but without nulls
 */
arrow::Status writeExamplesBase() {
    std::shared_ptr<arrow::RecordBatch> recordBatch1 = createDemoRecordBatch(100, true, false);
    std::shared_ptr<arrow::RecordBatch> recordBatch2 = createDemoRecordBatch(200, true, false);

    std::shared_ptr<arrow::io::FileOutputStream> randomAccessOut = arrow::io::FileOutputStream::Open("test.arrow.feather").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> randomAccessWriter = arrow::ipc::MakeFileWriter(randomAccessOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(randomAccessWriter->Close());

    std::shared_ptr<arrow::io::FileOutputStream> ipcStreamOut = arrow::io::FileOutputStream::Open("test.arrow.ipc").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipcStreamWriter = arrow::ipc::MakeStreamWriter(ipcStreamOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->Close());

    return arrow::Status::OK();
}

/**
 * Create Feather and IPC files with two batches containing not nullable columns
 */
arrow::Status writeExamplesNotNullable() {
    std::shared_ptr<arrow::RecordBatch> recordBatch1 = createDemoRecordBatch(100, false, false);
    std::shared_ptr<arrow::RecordBatch> recordBatch2 = createDemoRecordBatch(200, false, false);

    std::shared_ptr<arrow::io::FileOutputStream> randomAccessOut = arrow::io::FileOutputStream::Open("test-not-nullable.arrow.feather").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> randomAccessWriter = arrow::ipc::MakeFileWriter(randomAccessOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(randomAccessWriter->Close());

    std::shared_ptr<arrow::io::FileOutputStream> ipcStreamOut = arrow::io::FileOutputStream::Open("test-not-nullable.arrow.ipc").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipcStreamWriter = arrow::ipc::MakeStreamWriter(ipcStreamOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->Close());

    return arrow::Status::OK();
}

/**
 * Create Feather and IPC files with two batches containing nullable columns with some nulls
 */
arrow::Status writeExamplesWithNulls() {
    std::shared_ptr<arrow::RecordBatch> recordBatch1 = createDemoRecordBatch(100, true, true);
    std::shared_ptr<arrow::RecordBatch> recordBatch2 = createDemoRecordBatch(200, true, true);

    std::shared_ptr<arrow::io::FileOutputStream> randomAccessOut = arrow::io::FileOutputStream::Open("test-with-nulls.arrow.feather").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> randomAccessWriter = arrow::ipc::MakeFileWriter(randomAccessOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(randomAccessWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(randomAccessWriter->Close());

    std::shared_ptr<arrow::io::FileOutputStream> ipcStreamOut = arrow::io::FileOutputStream::Open("test-with-nulls.arrow.ipc").ValueOrDie();
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipcStreamWriter = arrow::ipc::MakeStreamWriter(ipcStreamOut, recordBatch1->schema()).ValueOrDie();
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch1));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->WriteRecordBatch(*recordBatch2));
    ARROW_RETURN_NOT_OK(ipcStreamWriter->Close());

    return arrow::Status::OK();
}

arrow::Status writeAllExamples() {
    ARROW_RETURN_NOT_OK(writeExamplesBase());
    ARROW_RETURN_NOT_OK(writeExamplesNotNullable());
    ARROW_RETURN_NOT_OK(writeExamplesWithNulls());
    return arrow::Status::OK();
}

} //end dataframe_example

int main(int argc, char* argv[]) {
    arrow::Status status = dataframe_example::writeAllExamples();
    if (status == arrow::Status::OK()) {
        return 0;
    } else {
        std::cerr << "Error (" << status.CodeAsString() << "): " << status.message() << std::endl;
        return 1;
    }
}

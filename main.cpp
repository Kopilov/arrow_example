#include <iostream>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

std::shared_ptr<arrow::Int64Builder> getInt64SequenceBuilder(long size) {
    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>();

    for (long i = 0; i < size; i++) {
        builder->Append(i);
    }
    return builder;
}

void addExample(
    std::string name,
    long size,
    std::function<std::shared_ptr<arrow::ArrayBuilder>(long)> prepareExampleBuilder,
    std::vector<std::shared_ptr<arrow::Field>> &fieldsExamples,
    std::vector<std::shared_ptr<arrow::Array>> &dataExamples
) {
    std::shared_ptr<arrow::ArrayBuilder> builder = prepareExampleBuilder(size);
    fieldsExamples.push_back(field(name, builder->type()));
    dataExamples.push_back(builder->Finish().ValueOrDie());
}

std::shared_ptr<arrow::RecordBatch> createDemoRecordBatch() {
    long size = 1000;

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> data;

    addExample("longValues", size, getInt64SequenceBuilder, fields, data);
    std::shared_ptr<arrow::RecordBatch> recordBatch = arrow::RecordBatch::Make(arrow::schema(fields), size, data);
    return recordBatch;
}

void writeAndClose(std::shared_ptr<arrow::ipc::RecordBatchWriter> writer, std::shared_ptr<arrow::RecordBatch> recordBatch) {
    writer->WriteRecordBatch(*recordBatch);
    writer->Close();
}

int main(int argc, char* argv[]) {
    std::shared_ptr<arrow::RecordBatch> recordBatch = createDemoRecordBatch();

    std::shared_ptr<arrow::io::FileOutputStream> randomAccessOut = arrow::io::FileOutputStream::Open("testFeather.arrow").ValueOrDie();
    writeAndClose(arrow::ipc::MakeFileWriter(randomAccessOut, recordBatch->schema()).ValueOrDie(), recordBatch);

    std::shared_ptr<arrow::io::FileOutputStream> ipcStreamOut = arrow::io::FileOutputStream::Open("testIPC.arrow").ValueOrDie();
    writeAndClose(arrow::ipc::MakeStreamWriter(ipcStreamOut, recordBatch->schema()).ValueOrDie(), recordBatch);

    return 0;
}

#include <iostream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

int main(int argc, char* argv[]) {//
    std::shared_ptr<arrow::io::FileOutputStream> out = arrow::io::FileOutputStream::Open("test.arrow").ValueOrDie();
    //arrow::ipc::MakeStreamWriter(out);
    //arrow::ipc::MakeFileWriter(out);
    return 0;
}

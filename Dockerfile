FROM ubuntu:jammy

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install gcc g++ cmake git
COPY arrow/ /usr/src/arrow
RUN cd /usr/src/arrow/cpp; cmake .; make -j$(nproc); make install; ldconfig
COPY main.cpp vectorsExamples.cpp vectorsExamples.h CMakeLists.txt /usr/src/arrow_examples/
RUN cd /usr/src/arrow_examples/; cmake .; make -j$(nproc); make install


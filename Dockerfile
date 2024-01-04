FROM ubuntu:jammy

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install gcc g++ cmake git
COPY arrow/ /usr/src/arrow
RUN cd /usr/src/arrow/cpp; cmake .; make; make install
COPY main.cpp vectorsExamples.cpp vectorsExamples.h CMakeLists.txt /usr/src/arrow_examples/
RUN cd /usr/src/arrow_examples/; cmake .; make; ./arrow_examples

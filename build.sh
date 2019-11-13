#export CC=/usr/local/gcc-4.9.3/bin/gcc
#export CXX=/usr/local/gcc-4.9.3/bin/g++

mkdir build
cd build
#cmake -DCMAKE_C_COMPILER=/usr/local/gcc-4.9.3/bin/gcc -DCMAKE_CXX_COMPILER=/usr/local/gcc-4.9.3/bin/g++ ..
cmake ..
make -j32
cp ../deps/tokenizer/etc/wwsearch_* .

# run example by using mock(simple memory) db
./wwsearch_example

# run example by using rocksdb
mkdir /tmp/db
./wwsearch_example /tmp/db

# run ut
./wwsearch_ut

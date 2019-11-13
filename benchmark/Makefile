
LIB = -I.. / include -
      L.. / build.. / build /
          librocksdb.a

              all : clean testdoclistsize teststorage rocksdb_tool echo ""

                    teststorage : echo $ @g++ -
      std = c++ 11 - g storage_test.cpp util.cpp iutf8string.cpp codec.cpp -
            o $ @${LIB} - lsnappy - lpthread

                                        testdoclistsize : echo $ @g++ -
                                                          std =
                c++ 11 - g doclist_test.cpp util.cpp iutf8string.cpp - o $ @

                rocksdb_tool : echo $ @g++ -
                               std =
                    c++ 11 - g rocksdb_tool.cpp util.cpp iutf8string.cpp -
                    o $ @${LIB} - lsnappy - lpthread

                                                clean
                    : rm -
                      rf teststorage testdoclistsize rocksdb_tool

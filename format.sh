dos2unix */*.h
dos2unix */*.cpp

clang-format -i -style=Google example/*.h
clang-format -i -style=Google example/*.cpp
clang-format -i -style=Google benchmark/*.h
clang-format -i -style=Google benchmark/*.cpp
clang-format -i -style=Google include/*.h
clang-format -i -style=Google src/*.cpp
clang-format -i -style=Google unittest/*.cpp

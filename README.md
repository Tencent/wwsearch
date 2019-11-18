![wwsearch-logo](./wwsearch-logo.png)


## `wwsearch`简介

`wwsearch`是企业微信后台自研的全文检索引擎。它为海量用户下的全文快速检索而设计，底层支持可插拔的`lsm tree`存储引擎。目前覆盖企业微信所有在线检索场景：企业员工通讯录、审批、日报、周报、汇报、企业素材检索，也包括企业邮箱的全文邮件检索。 最大业务场景有300+亿条记录，索引词项万亿+，存储容量几十TB，支撑实时在线用户检索。

## 功能介绍
1. **实时数据修改**：数据写入即实时可查。对外提供插入、更新、删除、覆盖写等接口，可适应更新频繁场景，也适应于少改或不改场景。
2. **支持灵活Query**：支持词的等值、前缀、模糊匹配。多个Query通过And 、Or进行组合，满足不同场景的检索需求。Query还可以按指定field进行检索。
3. **后置过滤**：支持对检索索引后的结果进行二次过滤，支持等值、数值范围、数组元素查找、字符串模糊等过滤特性。适用于如无法建立高区分度索引的字段过滤、带有业务特定场景的过滤。
4. **灵活排序**：支持按多个field的属性值组合排序，类似order by语义。
5. **检索功能可扩展**：场景需要时，可以扩展各类聚合函数（sum/avg…）,也可以支持场景文本打分。

## 实现剖析
* [全文检索引擎wwsearch实现剖析](doc/wwsearch-implement.md)

## 接口说明
具体使用例子参考`example/example.cpp`。 这里简单对接口字段进行说明。

### Index
主要涉及6个接口，分别是：
1. AddDocuments：仅当文档id不存在时添加；
2. UpdateDocuments：仅当文档id存在时更新；更新时会保留旧文档存在的未更新field内容；
3. AddOrUpdateDocuments：若文档id不存在则添加，若存在则更新；
4. ReplaceDocuments：仅当文档id存在时替换；
5. DeleteDocuments：仅当文档id存在时删除；
6. AddDocumentsWithoutRead：文档id不存在则添加，存在则覆盖；

下面以用户常用的`AddOrUpdateDocuments`为例说明用法。
```
// wwsearch/index_writer.h
bool AddOrUpdateDocuments(const TableID &table,
                          std::vector<DocumentUpdater *> &documents,
                          std::string *store_buffer = nullptr,
                          SearchTracer *tracer = nullptr);

// wwsearch/document.h
class DocumentUpdater {
    ...
  Document new_document_;
    ...
};

class Document {
    ... 
  std::vector<IndexField *> fields_;
  DocumentID document_id_;
    ...
};

// wwsearch/index_field.h
class IndexField {
    ...
  FieldID field_id_;
  IndexFieldFlag field_flag_;
  kIndexFieldType field_type_;
  uint64_t numeric_value_;
  std::string string_value_;
    ...
}
```
用户使用涉及主要字段说明:
1. TableID : bussiness_type(uint8_t) + partition_set(uint64_t)组成，分表；
2. DocumentID : uint64_t，文档id，文档的唯一标识；
3. IndexField ： 文档列的信息，包括列属性和值。
	- field_id_，field的ID
	- field_flag_，索引标记
		- kTokenizeFieldFlag，是否分词
		- kStoreFieldFlag，是否存储原始数据
		- kDocValueFieldFlag，是否存储列值属性
	    - kSuffixBuildFlag，是否后缀展开
	    - kInvertIndexFieldFlag，是否建立倒排索引
    - field_type_，值类型
	    - kUint32IndexField
	    - kUint64IndexField 
	    - kStringIndexField 
	- numeric_value_/ string_value_，字段原始值

### Query
主要涉及接口：
```
// wwsearch/searcher.h
SearchStatus DoQuery(const TableID &table, Query &query, size_t top,
                     std::vector<Filter *> *filter,
                     std::vector<SortCondition *> *sorter,
                     std::list<DocumentID> &docs,
                     uint32_t min_match_filter_num = 0)
```
用户使用涉及主要字段说明：
1. TableID : bussiness_type(uint8_t) + partition_set(uint64_t)组成，分表；
2. Query ：构建查询的字段信息，可支持AndQuery和OrQuery的嵌套格式，支持PrefixQuery前缀查询；参考
3. Filter ：过滤器，支持数字/字符串/数组/多字符串条件过滤；
4. SortCondition ：对查询得到的文档输出做排序，支持指定field做排序，目前只支持指定数字的field排序；
5. min_match_filter_num设置最小匹配的filter数，只要匹配的filter大于此数的文档才能输出。

## 构建方法
### 依赖模块说明
依赖模块为：
```
# wwsearch/deps/
protobuf-2.4.1
snappy-1.0.4
rocksdb-v5.16.6
tokenizer-mmseg
```
仓库中已提前编译生成依赖库，您也可以根据编译环境重新编译依赖的第三方模块。

### 构建方法：
需要使用支持c++ 11的编译环境构建
```
mkdir build
cd build
cmake  ..
make -j32
cp ../deps/tokenizer/etc/wwsearch_* .
```
编译完成将可以看到：
1. wwsearch_ut :  单元测试；
2. wwsearch_example : 简单示例，包括index和query。

接下来可以愉快使用啦，enjoy it!

## 贡献代码
提交pull request贡献代码前，请参考 [Contributing.md](./Contributing.md) 。
`wwsearch`基于c++11开发，遵循[Google C++ Style Guide代码风格](https://google.github.io/styleguide/cppguide.html)，提交代码前需要使用附带的`.clang-format`格式化代码；

## 反馈问题
使用中遇到问题，可以有以下途径反馈：
1. 直接在[issues]提问；

## 开源协议

wwsearch 开源协议为 Apache License Version 2.0 ，详细的 License 请参考 [LICENSE.TXT](./LICENSE.TXT)



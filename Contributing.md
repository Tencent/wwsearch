# Contributing

我们提倡您通过提 issue 和 pull request 方式来促进 wwsearch 的发展。

## Acknowledgements

wwsearch 持续招募贡献者，即使是在 issue 中回答问题，或者做一些简单的 bugfix ，也会给 wwsearch 带来很大的帮助。
              
## Issue 提交

#### 对于贡献者

在提 issue 前请确保满足一下条件：

- 必须是一个 bug 或者功能新增。
- 已经在 issue 中搜索过，并且没有找到相似的 issue 或者解决方案。

##  Pull request

我们除了希望听到您的反馈和建议外，我们也希望您接受代码形式的直接帮助，对我们的 GitHub 发出 pull request 请求。

以下是具体步骤：

#### Fork仓库

点击 `Fork` 按钮，将需要参与的项目仓库 fork 到自己的 Github 中。

#### Clone 已 fork 项目

在自己的 github 中，找到 fork 下来的项目，git clone 到本地。

```bash
$ git clone git@github.com:<yourname>/wwsearch.git
```

#### 添加 wwsearch 仓库

将 fork 源仓库连接到本地仓库：

```bash
$ git remote add <name> <url>
# 例如：
$ git remote add wwsearch git@github.com:Tencent/wwsearch.git
```

#### 保持与 wwsearch 仓库的同步

更新上游仓库：

```bash
$ git pull --rebase <name> <branch>
# 等同于以下两条命令
$ git fetch <name> <branch>
$ git rebase <name>/<branch>
```

#### 开发调试代码

您需要在Linux平台编译执行。
```bash
mkdir build
cd build
cmake  ..
make -j32
cp ../deps/tokenizer/etc/wwsearch_* .
```

编译完成将可以看到：

wwsearch_ut : 单元测试；
wwsearch_example : 简单示例，包括index和query。
接下来可以愉快使用啦，enjoy it!

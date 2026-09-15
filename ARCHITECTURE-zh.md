# 架构

[English](ARCHITECTURE.md)

SDK 对外保持一套内聚的对象模型，对内按 Cloud 和 runtime 的所有权划分实现。应用只需
导入根 `ags` 包，也可以按需使用 `sandbox` 快捷包。

## 公共模型

```text
Client -> SandboxManager -> Sandbox
                              |-- Files
                              |-- Commands
                              |-- PTY
                              |-- Code
                              `-- Metrics
```

一个 `Client` 绑定一套腾讯云身份、地域、Endpoint 和请求策略。Create 和 Connect 都返回
唯一的 `*ags.Sandbox`。`sandbox` 包只是基于环境变量的 Client 快捷入口，不包含另一套
Sandbox 实现。

## 依赖方向

```text
ags / sandbox（公共 facade）
        |
        +--> internal/controlplane --> 腾讯云官方 SDK
        |
        `--> internal/runtime --> internal/dataplane --> internal/gen
                    |
                    `--> internal/model
```

- 根包负责公共选项、参数校验、稳定错误，以及公共模型和内部 DTO 的转换。
- `internal/controlplane` 负责 Cloud Client、签名集成、Action 映射、生命周期轮询、
  Metrics 请求、实例 Token 获取和新 runtime generation 的构造。
- `internal/runtime` 负责 generation 有效性、请求生命周期、handle、stream pump、有界队列、
  输出聚合和本地失效。
- `internal/dataplane` 负责不可变 Endpoint 和实例访问材料、鉴权 Header、HTTP/Connect 调用、
  协议解析和 start barrier。
- `internal/gen` 保存生成的协议绑定，只允许数据面实现和协议测试夹具引用。
- `internal/model` 只保存多个内部层确实需要共享的最小 wire-independent DTO 和错误词汇，
  不 alias 公共类型或生成类型。

内部包不得反向导入根 `ags` 包。根目录生产代码不得导入 Connect、生成绑定或腾讯云生成 SDK。

## 生命周期所有权

每个已连接的 Sandbox 只拥有一个当前数据面 generation。Reader、Watch、Command handle、
PTY、Code execution 和受管理 Code context 都属于创建它们的 generation。

- Pause 在提交 Cloud mutation 前使当前 generation 失效。
- Resume 重新获取实例访问材料，并在数据面 ready 后安装新 generation。
- Close 同步终止本地工作，不删除远端实例。
- Delete 是显式远端清理操作。
- 调用方取消 context 只停止本地观察，不能证明已接受的远端副作用被撤销。

正常关闭流、generation 失效和显式远端进程信号是三种不同操作。跨层移动实现时不得合并
这些语义。

## 增加能力

1. 只有用户需要稳定契约时，才新增 SDK 自有公共输入和结果类型。
2. 私有 DTO 优先放在能力所属的内部包；只有多个内部层都需要时才放入 `internal/model`。
3. 为数据面增加语义操作，不暴露 Client、Endpoint、Token、生成消息或通用 Request。
4. 生命周期和有界异步行为放入 `internal/runtime`，公共 handle 只转换和委托。
5. 使用确定性的 loopback 测试覆盖映射、取消、上限、错误和远端副作用语义。真实 Cloud
   测试继续要求显式启用。
6. 运行 `make verify`。仓库检查器会验证依赖方向和根目录公共职责白名单。

公共 API 变化需要先讨论，并审查 API 快照、迁移说明和 Changelog。纯内部重构必须保持
公共 API 快照不变。

## 验证

默认验证完全离线，覆盖格式、单测、vet、race、公共消费者编译、API 快照、生成漂移、
仓库架构和敏感信息检查。可选真实 Cloud 验证及其环境变量见
[test/README.md](test/README.md)。

贡献流程见 [CONTRIBUTING-zh.md](CONTRIBUTING-zh.md)。

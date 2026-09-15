# 贡献指南

[English](CONTRIBUTING.md)

本仓库接受缺陷报告、范围明确的功能建议、测试、文档和代码修改。

## 提交修改前

- 先检索已有 Issue 和 Pull Request。
- 一个修改只解决一个问题。
- 公共 API 变更应先讨论范围和迁移影响。
- 不要提交凭证、沙箱实例访问材料、真实资源 ID、私有 Endpoint、客户数据或云测试输出。
- 安全问题按 [SECURITY.md](SECURITY.md) 的私密渠道报告。

## 开发环境

- Go 1.22 或更高版本。CI 使用 Go 1.22 和 `GOTOOLCHAIN=local` 验证。
- 使用 Go 1.25.4 或更高版本运行 `make tools`，安装固定版本的生成器和 Gitleaks。这些构建
  工具要求的 Go 版本高于 SDK；SDK 门禁仍使用 Go 1.22.12 和 `GOTOOLCHAIN=local`。

从上游 `main` 创建分支：

```bash
git clone https://github.com/YOUR_ACCOUNT/ags-go-sdk.git
cd ags-go-sdk
git remote add upstream https://github.com/TencentCloudAgentRuntime/ags-go-sdk.git
git switch -c feature/short-description upstream/main
```

## 修改要求

- 公共签名只使用 SDK 自有类型。Cloud 生成类型、protobuf 和 Connect 类型只在内部适配层使用。
- 为所有导出符号补充 GoDoc。
- 新行为需要确定性的离线测试，并覆盖取消、上限和错误。
- 异步使用 map、slice 或指针字段前，先复制调用方数据。
- 不要为 mutation、命令、上传或流操作增加自动重试。
- 公共 API 或行为变化需要更新 `MIGRATION.md` 和 `CHANGELOG.md`。
- 中英文文档需要保持内容一致。

运行完整离线门禁：

```bash
make verify
```

默认测试不得访问腾讯云。

## 包边界

修改内部边界前，请先阅读 [架构说明](ARCHITECTURE-zh.md)。

面向用户的 API 保持内聚，协议实现放在私有边界内：

- 根 `ags` 包定义 `Client`、`Sandbox`、能力入口、选项、结果和稳定错误。新增一个功能本身
  不是增加公共 package 的理由。
- 公共 `sandbox` 包只提供基于环境变量的快捷函数和类型别名，并始终返回根包定义的
  `*ags.Sandbox`。
- `internal/controlplane` 负责 Cloud/Monitor Client、Action 映射、生命周期轮询、Cloud 错误
  归一化和 runtime generation 获取。
- `internal/runtime` 负责 Sandbox generation、handle 生命周期、stream pump、有界队列、聚合
  和本地失效。
- `internal/model` 只保存内部层共享的 wire-independent DTO 和错误分类，不得 alias 公共类型
  或生成类型。
- `internal/dataplane` 负责不可变的运行时 Endpoint、实例访问材料、鉴权 Header、底层
  HTTP/Connect Client、生成消息构造、start barrier、wire event 解析和私有语义 DTO。
- `internal/gen` 保存 Filesystem 和 Process 生成代码，只允许 `internal/dataplane` 或协议测试
  夹具引用。根目录生产代码不得 import Connect 或生成代码。

不要仅为了缩短文件，就把 Files、Commands、Code、PTY 或 Metrics 拆成多个公共 package。
应在保持 `Client -> Sandbox` 用户模型不变的前提下，把私有实现拆入 `internal/`。

不要从私有 Client 暴露底层 getter 或泛型请求构造器。应在 `internal/dataplane` 增加语义操作；
`make verify` 会检查该依赖方向。

## 协议修改

协议文件来自已记录来源。修改前完成以下操作：

1. 确认上游 revision 和许可证。
2. 保留修改声明和 module package mapping。
3. 更新 `contracts/proto.json` 中的哈希。
4. 运行 `make generate` 并审查生成代码 diff。
5. 运行 `make verify-generate` 和 `make verify`。

不要手工修改 `internal/gen/` 下的生成文件。

## 可选真实云测试

真实云测试需要显式启用，不属于 `go test ./...`。请使用专用测试账号和 Tool。最小环境变量为：

```bash
export TENCENTCLOUD_SECRET_ID=your-test-secret-id
export TENCENTCLOUD_SECRET_KEY=your-test-secret-key
export AGS_E2E_REGION=ap-guangzhou
export AGS_E2E_TOOL_ID=your-test-tool-id
export AGS_E2E_CODE_TOOL_ID=your-code-interpreter-tool-id
export AGS_E2E=1
```

仅在使用腾讯云临时凭证时设置 `TENCENTCLOUD_TOKEN`。Metrics 与其他旅程测试共用凭证、
地域和实例。

串行运行云测试：

```bash
make test-e2e
```

每个测试只清理自己创建的实例，使用独立 cleanup context，并确认实例进入终态。不要对共享
生产资源运行测试，也不要修改共享 Tool。

## Pull Request

Commit 标题使用 Conventional Commits，例如 `feat(sandbox): add bounded watch stream`。Pull
Request 需要说明用户可见行为、迁移影响、已运行测试、协议或依赖变化和剩余限制。不要在
Pull Request 中发布 tag 或 module 版本。

贡献内容使用仓库的 [Apache-2.0 许可证](LICENSE)。

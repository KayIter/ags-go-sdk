# Agent Sandbox Go SDK

[English](README.md)

这是腾讯云 Agent Sandbox 的 Go SDK。SDK 通过同一个 `Sandbox` 对象提供沙箱生命周期、
流式文件、命令、PTY、文件监听、代码执行上下文和十项云监控指标。

当前分支对应下一版 1.0 前 API。从 v0.1.5 或更早版本升级前，请先阅读
[迁移指南](MIGRATION.md)。

## 使用条件

- Go 1.22 或更高版本。
- 已开通腾讯云 Agent Sandbox。
- 目标地域中已有可用的 Sandbox Tool。
- 运行环境可以访问腾讯云 API 和沙箱数据面。

## 安装

```bash
go get github.com/TencentCloudAgentRuntime/ags-go-sdk
```

## 鉴权

控制面和 Metrics 使用腾讯云凭证：

```bash
export TENCENTCLOUD_REGION=ap-guangzhou
export TENCENTCLOUD_SECRET_ID=your-secret-id
export TENCENTCLOUD_SECRET_KEY=your-secret-key
# 使用腾讯云临时凭证时设置：
export TENCENTCLOUD_TOKEN=your-session-token
```

Create、Connect 或 Resume 完成后，SDK 通过控制面取得沙箱实例访问材料。该材料只在
SDK 内部使用。公共 API 不接收或返回该材料，也不会把它写入 URL。

## 快速开始

单一云身份场景可以使用 `sandbox` 包。它读取并缓存上述环境配置：

```go
package main

import (
	"context"
	"log"
	"time"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/sandbox"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	lifetime := 10 * time.Minute
	sb, err := sandbox.Create(ctx, sandbox.CreateOptions{
		Tool:    sandbox.ToolRef{ID: "your-tool-id"},
		Timeout: &lifetime,
		Env:     map[string]string{"APP_ENV": "demo"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sb.Close() // 只释放本地资源

	result, err := sb.Commands().Run(ctx, "sh", ags.CommandOptions{
		Args: []string{"-lc", "printf 'hello from sandbox'"},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("exit=%d stdout=%s", result.ExitCode, result.Stdout)

	cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	if err := sb.Delete(cleanup); err != nil {
		log.Fatal(err)
	}
}
```

## 显式 Client 与多云身份

需要明确传入凭证、地域或传输配置时，使用 `ags.Client`。一个 Client 只绑定一个逻辑
云身份。同一进程使用多个云账号时，应为每套凭证分别创建 Client。

```go
client, err := ags.NewClient(
	ags.WithRegion("ap-guangzhou"),
	ags.WithCredentialProvider(provider),
)
if err != nil {
	return err
}

sb, err := client.Sandboxes().Connect(ctx, sandboxID)
if err != nil {
	return err
}
defer sb.Close()
```

快捷入口和显式 Client 都返回 `*ags.Sandbox`，并调用同一套控制面、Metrics、生命周期
和数据面实现。

## 能力

| 范围 | API |
| --- | --- |
| 控制面 | `Create`、`Connect`、`Get`、`List`、`Pause`、`Resume`、`WaitFor`、`Update`、`Delete` |
| 文件 | 流式 `Read`/`Write`、`Stat`、`Exists`、`List`、`MakeDir`、`Move`、`Remove`、`Watch` |
| 命令 | `Run`、`Start`、`List`、`Connect`、有界输出、输入和信号 |
| Code | `Run`、受管理 Context、显式外部 Context 引用、有界回调 |
| PTY | 启动屏障、输入、窗口调整、有界事件流 |
| Metrics | 十项类型化指标，支持 `Start`、`End`、采样周期和部分成功 |

## 生命周期规则

- `Sandbox.Close()` 只释放本地 reader、流和观察任务，不删除远端实例。
- `Sandbox.Delete(ctx)` 显式停止远端实例，NotFound 按幂等成功处理。
- Pause 在提交控制面请求前使当前数据面 generation 失效。Resume 获取新的连接材料。旧
  reader、handle、watch、PTY、Code execution 和受管理 Context 不会恢复。
- context 取消只停止本地观察，不能证明远端 mutation 或进程已撤销。
- SDK 不自动重试 mutation。

Create 和 Resume 接受 30 秒至 24 小时的整秒 Timeout。Update 最低为 300 秒。Connect
先读取实例状态：PAUSED 使用 Resume 规则，RUNNING 使用 Update 规则。SDK 会原样发送
合法值；已部署的 Cloud 策略仍可能拒绝 30～299 秒，此时 SDK 返回原始结构化服务错误，
不会提升参数或自动重试。

## 错误处理

使用 `errors.As` 读取 `*ags.Error`。稳定字段包括 `Code`、`Reason`、`Operation`、
`RequestID`、`Retryable`、`InstanceID` 和可选 mutation 证据。格式化错误不会输出凭证、
授权头、实例 ID、Cause 或响应正文。

## 开发

```bash
make verify
```

默认测试不会访问云端。真实云测试必须显式启用，具体变量和清理要求见
[贡献指南](CONTRIBUTING-zh.md)。协议与控制面来源记录在 [`contracts/`](contracts/) 中。

实现边界见 [架构说明](ARCHITECTURE-zh.md)，更多示例见
[Cookbook](examples/cookbook/README.md)。安全问题请按 [安全策略](SECURITY.md) 报告。

## 许可证

本项目使用 Apache License 2.0。详见 [LICENSE](LICENSE)、[NOTICE](NOTICE) 和
[SOURCE_ATTRIBUTION.md](SOURCE_ATTRIBUTION.md)。

# RiverBridge

> **简体中文** ｜ [English](README.md)

一个 [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI)（简称 CPA）插件，
把 [River AI](https://docs.river.ai) 的推理能力接入 CPA，变成一个一等公民级别的供应商。

只需配置一次 River API Key：RiverBridge 会把 River 的模型以 `river-` 前缀发布到 CPA 的
`/v1/models` 里，负责把 chat completions 转发到上游，并按 key 记录 token 用量与花费 ——
全部通过 CPA 自己托管的一个小网页控制台完成。

```
river-deepseek-v4.1-flash-1017   deepseek-ai/DeepSeek-V4.1-Flash-1017
river-glm-5.3-flash              zai-org/GLM-5.3-Flash
river-kimi-k3                    moonshotai/Kimi-K3
```

## 为什么要有这个插件

CLIProxyAPI 的每个供应商都各管各的凭据、各算各的额度、各自有一套「这个模型多少钱」的
认知。River 便宜到值得绕进去用，但没办法把一把 River Key 丢给 CPA 然后就不管了：
上一笔请求是哪把 key 付的钱、余额还剩多少、六个模型里哪几个值得留着，都得找个 CPA 之外
的地方存着。

RiverBridge 就是这个「地方」。它起于一份个人配置 —— 一把 key、一个消费上限、一份重启后
还在的日志 —— 之所以公开，是因为任何人拿 CPA 对接按量计费的上游时，想要的东西都是这个形状。
它假设**只有一个运维人员**，不是一支团队：没有多租户，没有 RBAC，控制台直接信任 CPA 自己的
管理密钥来决定谁能进来。

## 为什么加 `river-` 前缀

CPA 在所有供应商之间共享**一个扁平**的模型命名空间。River 提供的模型短名会和别家撞车 ——
比如 `deepseek-v4.1-flash` 在别处也存在。所以 RiverBridge 注册的每个模型都带 `river-` 前缀，
`river-deepseek-v4.1-flash` 就明确是 River 的。

前缀是在**去掉厂商段之后**才加到 slug 上的：River 的
`deepseek-ai/DeepSeek-V4.1-Flash-1017` 会变成 `river-deepseek-v4.1-flash-1017`。
往回转发时会做反向映射，所以 River 收到的永远是原始 id。

## 安装

RiverBridge 和其他 CPA 插件一样，就是一个 C 共享库。

```sh
# FreeBSD（CPA 常见的宿主机）
gmake freebsd

# 把产物拷进插件的扫描目录
scp dist/riverbridge.so user@host:~/cpa/plugins/freebsd/amd64/riverbridge.so
```

然后在 `config.yaml` 里启用：

```yaml
plugins:
  enabled: true
  dir: "plugins"
  configs:
    riverbridge:
      enabled: true
      base_url: "https://api.river.ai/v1"
      request_timeout_seconds: 600
      log_retention: 1000
```

重启 CPA，留意这行注册日志：

```
pluginhost: plugin registered plugin_id=riverbridge plugin_name=RiverBridge version=0.3.1
```

## 使用

从 CPA 的管理界面进控制台 —— RiverBridge 会在插件资源里注册一个 **RiverBridge** 条目 ——
或者直接访问：

```
/v0/resource/plugins/riverbridge/console
```

粘贴一把 River Key（`rv_...`）。RiverBridge 会**先**向上游探一次，确认可用后才往磁盘写东西，
所以一把废 key 永远不会进到 auth 目录里。这把 key 随后变成一条普通的 CPA auth 记录，
控制台里能看到它的模型、token 用量和累计花费。

控制台分四个标签页：

- **日志 (logs)** —— 每一次请求，最新的在最前，可按级别、账号、模型和自由文本过滤。
  分页显示，支持导出 NDJSON。默认只看「今天」+「仅请求」，避免一打开先被
  `settings saved ...` 之类的系统事件淹没。
- **账号配置 (accounts)** —— 每把 key 一张卡：启用/停用、改名、设优先级或消费上限、
  单独关掉某个模型。被关掉的模型会直接从这把 key 的 `/v1/models` 里消失，
  而不是等请求打过来才失败。
- **路由策略 (routing)** —— fill-first（永远用优先级最高的那把可用 key）或
  round-robin（在多把 key 之间分摊），外加失败重试。
- **设置概览 (settings)** —— 运行时概览、日志保留条数、请求超时、以及全局屏蔽的模型。

控制台的 API 调用打到 `/v0/management/riverbridge/*`，由 CPA 用管理密钥守卫。
如果你在 CPA 管理中心勾了「记住凭据」，RiverBridge 会复用那个会话；否则弹解锁框。

然后照常用 CPA 聊天：

```sh
curl http://127.0.0.1:56466/v1/chat/completions \
  -H "Authorization: Bearer $CPA_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"river-deepseek-v4.1-flash-1017","messages":[{"role":"user","content":"hi"}]}'
```

### 消费上限

River **没有余额接口** —— 每一条说得通的路径（`/v1/credits`、`/v1/balance`、`/v1/usage`，
以及那些根本不可能存在的路径）都返回 `200` 加一个**空 body** —— 所以 RiverBridge
问不出来一把 key 还剩多少额度。于是每把 key 可以带一个**美元消费上限**，
插件用和花费列同一套「每百万 token 单价」来累加花销。设成 `0` 表示不设上限。

累计值存在 `plugins/riverbridge-data/spend.json`，**不是** auth 文件。CPA 每次请求都会重跑
`auth.parse` 并**用文件内容重建**这条记录，所以存在 auth 文件里的计数器会在计费后一秒内被清零。

某把 key 到达上限后会被**移出轮换**并在控制台标记出来。它不会被回 `403`：CPA 会把 403
记在那条 auth 上并持续跳过它，那样即使充值了这把 key 也会一直“黑着”。抬高上限
（或者充值后按一下「重置已用」）会让它在**一次请求之内**恢复服务，不需要重启。

### 用量是实测的，不是估的

River 的 OpenAI 兼容流会在**最后一帧**带上 `usage`（RiverBridge 主动请求
`stream_options.include_usage` 以确保如此），所以日志和花费列里的 token 数都是
**上游实测值**。`internal/bridge/auth.go` 里那句注释就是这个意思：
*"Everything here is measured or reported, never estimated."*

上游万一没给 usage，日志会写 `no usage reported`，而不是编一个数字出来。
（这一点和 SystalyzeBridge 不同：Systalyze 的流从不报 usage，那边才需要用字符数估算。）

## 配置

所有字段都在 `plugins.configs.riverbridge` 下，且都可以通过控制台在运行时覆盖 ——
控制台会把改动持久化到 `plugins/riverbridge-data/settings.json`。

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `data_dir` | `plugins/riverbridge-data` | 插件状态（settings、usage） |
| `base_url` | `https://api.river.ai/v1` | 所有凭据共用的上游地址 |
| `strategy` | `fill-first` | `fill-first` 或 `round-robin` |
| `retry_on_failure` | `true` | 失败的 key 冷却一会儿，换一把再试 |
| `disabled_models` | `[]` | 在所有 key 上统一屏蔽的模型 |
| `log_retention` | `1000` | 磁盘上保留的请求行数（50–10000，与 ClinePassBridge 同范围） |
| `request_timeout_seconds` | `600` | 单次上游调用的超时 |
| `include_stream_usage` | `true` | 向 River 请求流式请求的 usage |

`data_dir` 在控制台里是**只读**的：它决定启动时从哪里加载状态，运行中改它会把已有的
key 全部丢在原地。

### 账号状态是怎么回到 CPA 的

如果你要改这个插件，有两处宿主行为值得知道。

凭据是**直接写 auth 文件**的，不走 `host.auth.save`。宿主会用「它刚写的那个文件」重建记录，
而这个重建过程会把 `disabled` 重置 —— 所以走 RPC 保存的做法，每次都会把一把已停用的 key
又打开。直接写文件能让文件保持权威，并让 CPA 的文件监听器重新解析它，和手工编辑完全一样。

屏蔽一个模型要改**每一把** key 的对外模型列表，却不该动任何一把 key，所以 RiverBridge
会给每条 auth 提一次模型版本号并重新发布文件。宿主是按**文件哈希**缓存模型列表的，
不这么做它会继续对外宣告你刚屏蔽掉的那个模型。

### 关于 scheduler 能力

RiverBridge 实现了 CPA 的 `scheduler.pick`，所以一个 key 池可以按请求来调度。
CPA **只认第一个**声明了 scheduler 的插件，而插件是按**文件名顺序**加载的 ——
这个位置可能被别的插件占着。路由页会显示实时的 pick 计数，所以你能看出究竟是
RiverBridge 在挑，还是选择被委托给了 CPA 内置策略。两种情况你选定的策略都仍然生效，
因为不合格的请求会**回落到内置行为**，而不是直接失败。

## 流式

流式调用是**逐帧代理**的，不做缓冲。RiverBridge 会请求 `stream_options.include_usage`
以便最后一帧带上 token 计数，会盯着 `[DONE]` 是否缺失，并且把**卡住超过五分钟**的流中断掉，
免得一个死掉的上游把连接永远占着。

## 计价

内置价目表（`internal/bridge/pricing.go`）对齐 River 公布的费率，这样控制台不用再发一次
API 调用就能显示花费。发布之后上游新增的模型在这里记 `0`，而不是猜一个数。

**给同时跑 `cpa-key-billing` 的 CPA 用户**：那个插件会拒绝它没有价目的模型
（`model_price_error`，HTTP 503）。新增一个 River 模型后，把它的价格注册一次：

```sh
curl -X PUT http://127.0.0.1:56466/v0/management/plugins/cpa-key-billing/prices \
  -H "X-Management-Key: $MGMT" -H 'Content-Type: application/json' \
  -d '{"model_id":"river-deepseek-v4.1-flash","input_per_1m":0.12,"output_per_1m":0.48,"cache_read_per_1m":0.0024}'
```

## 构建

```sh
go build ./...          # 编译检查
go test ./...           # 单元测试
gmake freebsd           # 给 FreeBSD/amd64 编 C 共享库
```

需要 Go 1.21+，编译共享库时还需要一个 C 编译器（`CGO_ENABLED=1`）。
`go.mod` 里写 1.26 是因为参考的 FreeBSD 构建用的是它；插件本身没有用到任何比 1.21 更新的
特性，所以老工具链也编得出来。

**不想在本地装 Go 的话**：本仓库的 GitHub Actions 用
`vmactions/freebsd-vm` 起一个 **FreeBSD 14.3** 虚拟机来编，产物以 artifact 形式提供；
打一个 `v*` tag 会自动发布 Release（含 `checksums.txt`），公开仓可以用 `curl -L -O` 直接下。
详见 [.github/workflows/ci.yml](.github/workflows/ci.yml)。

### 发布新版本

Release 说明是**手写的中文**，不是自动生成的提交列表。顺序是这样：

1. 改 `internal/bridge/types.go` 的 `Version`，功能改动 + 该版本的中文说明一起提交
2. 中文说明放在 `docs/release-notes/<tag>.md`（例如 `docs/release-notes/v0.3.1.md`）
3. `git tag -a v0.3.1 -m "..."` 然后 `git push origin main --follow-tags`

CI 会从 `docs/release-notes/<tag>.md` 读说明发布；文件不存在时回落到 `--generate-notes`
并打一条 warning，不会阻断发布。说明要**先于 tag 推上去**——workflow 取自 tag 指向的
commit，不是 main HEAD。

## 目录结构

```
cmd/riverbridge/main.go   CPA 用 dlopen() 加载的 C ABI 接口
internal/bridge/
  service.go              RPC 分派（插件协议）
  auth.go                 key → auth 记录、模型注册
  discovery.go            /v1/models 探测与校验
  executor.go             chat completion 代理、SSE 转发
  pricing.go              River 费率表
  management.go           控制台 API 与路由
  store.go                用量记账、日志环形缓冲
  ui/console.html         控制台页面
```

## 许可证

MIT —— 见 [LICENSE](LICENSE)。

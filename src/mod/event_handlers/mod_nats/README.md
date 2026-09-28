# mod_nats 模块设计文档

NATS 消息总线集成模块——为 FreeSWITCH 提供基于 NATS 的呼叫控制面（XCC 协议兼容）、事件流与 CDR 发布。目标是替代 ESL 成为外部系统集成的正门，实现**媒体与控制分层**。

## 1. 架构

```
┌───────────────────────────────┐            ┌────────────────────────────┐
│  FreeSWITCH (媒体节点)         │            │  外部控制器 (XCtrl)         │
│                               │   NATS     │  Java / Go / ...           │
│  RTP / 录音 / ASR/TTS(媒体面) │◄──────────►│  只收发控制消息,不碰媒体     │
│  mod_nats   (控制面)          │  JSON-RPC  │                            │
└───────────────────────────────┘            └────────────────────────────┘
        │                                               
        └── mod_event_socket 保持不动,仅作 fs_cli 运维通道
```

模块分层：

| 文件 | 职责 |
|------|------|
| `mod_nats.c` | 加载/卸载、配置解析、请求工作线程、`nats` CLI 命令 |
| `nats_conn.c` | cnats 连接管理（自动重连）、订阅分发、发布队列 |
| `nats_proto.c` | JSON-RPC 2.0 信封、方法路由、响应组装 |
| `nats_methods.c` | XNode.* 方法实现、Accept 通道绑定、bgapi 任务关联 |
| `nats_events.c` | Event.Channel / Event.CDR / Event.Result 事件发布 |

线程模型：cnats 回调线程只做拷贝入队（永不阻塞）；N 个请求工作线程执行 FS 调用；单发布线程排空发布队列，**队列满则丢弃并计数**（事件流 at-most-once，关键数据走 CDR/JetStream）。

## 2. Subject 布局

前缀可配置（`subject-prefix`），默认 `nats.fs.`。

| Subject | 方向 | 用途 |
|---------|------|------|
| `{prefix}node.{node_uuid}` | 控制器 → FS | JSON-RPC 请求入口 |
| `{prefix}ctrl.{ctrl_uuid}` | FS → 控制器 | 控制器信箱：Accept 后该通道的事件、Event.Result |
| `{prefix}event.channel.{STATE}` | FS → 广播 | 通道状态事件（START/RINGING/ANSWERED/BRIDGE/UNBRIDGE/DESTROY/MEDIA） |
| `{prefix}event.{event_name}` | FS → 广播 | 原生事件转发（默认关闭） |
| `{prefix}event.cdr` | FS → 广播 | CDR（建议服务端配 JetStream stream 持久化） |

**SDK 兼容性**：官方 xctrl Go SDK（`xswitch-cn/xctrl`）将 `cn.xswitch.` 前缀硬编码在 `ctrl/ctrl.go` 中，无配置接口。两种用法：
- 把本模块 `subject-prefix` 配成 `cn.xswitch.` → 官方 Go/Java SDK 即插即用；
- 坚持自有前缀 `nats.fs.` → fork SDK 改前缀（Apache 2.0/MIT 允许），或自研客户端。

## 3. 协议

XCC 风格 JSON-RPC 2.0（信封规范见 https://docs.xswitch.cn/xcc-api/design/ ，消息结构见 xswitch-cn/proto 的 xctrl.proto）：

```json
// 请求（控制器 → {prefix}node.{node_uuid}）
{"jsonrpc":"2.0","id":"call-1","method":"XNode.Answer",
 "params":{"ctrl_uuid":"my-ctrl","uuid":"efed526f-..."}}

// 响应（FS → 请求的 reply inbox）
{"jsonrpc":"2.0","id":"call-1",
 "result":{"code":200,"message":"OK","node_uuid":"..."}}

// 事件通知（无 id）
{"jsonrpc":"2.0","method":"Event.Channel",
 "params":{"node_uuid":"...","uuid":"...","state":"ANSWERED","caller_id_number":"1000",...}}
```

result.code 语义：200 成功 / 202 已受理（结果走 Event.Result）/ 400 拒绝 / 404 通道不存在 / 419 已被其他控制器接管 / 500 内部错误 / 501 未实现。

### 已实现方法（v0.1）

| 方法 | 说明 |
|------|------|
| XNode.Accept | 控制器接管通道（首个成功者获得控制权，后续返回 419） |
| XNode.Answer / Hangup | 应答 / 挂机（cause 可配，默认 NORMAL_CLEARING） |
| XNode.Play / Stop / Broadcast | 放音 / 停止放音 / 广播媒体 |
| XNode.Bridge / ChannelBridge | 桥接两条通道 |
| XNode.SetVar / GetVar / GetState / GetChannelData | 变量与状态读写 |
| XNode.Dial | 外呼（bgapi originate，立即回 202 + job_uuid，结果走 Event.Result） |
| XNode.JStatus | 节点状态：sessions/peak/sps/uptime/version |
| XNode.NativeApp / NativeAPI / NativeJSAPI | 逃生舱：任意 dialplan app / fs API / JSON API |
| Event.Channel / Event.CDR / Event.Result | 事件与异步结果 |

**未实现（规划中）**：UnBridge2、Transfer、Hold、ThreeWay、Mute、ReadDTMF、DetectSpeech（ASR，可对接 mod_ws_audio）、Record、Conference 系列、MediaFork——过渡期均可通过 NativeApp/NativeAPI 透传实现。

## 4. 配置（conf/autoload_configs/nats.conf.xml）

```xml
<configuration name="nats.conf" description="NATS Bus">
  <settings>
    <!-- 逗号分隔多 URL，客户端自动故障切换 -->
    <param name="urls" value="nats://127.0.0.1:4222,nats://127.0.0.1:4223"/>
    <!-- subject 前缀；要即插即用官方 xctrl SDK 改为 cn.xswitch. -->
    <param name="subject-prefix" value="nats.fs."/>
    <!-- 节点 ID，留空则每次加载生成随机 UUID（生产建议固定） -->
    <param name="node-uuid" value="fs-node-01"/>
    <!-- 认证三选一：账号密码 / JWT credentials 文件 -->
    <param name="user" value="fs"/>
    <param name="password" value="secret"/>
    <!-- <param name="credentials" value="/etc/freeswitch/nats.creds"/> -->
    <param name="publish-events" value="true"/>
    <param name="enable-cdr" value="true"/>
    <!-- 转发全部原生 FS 事件（默认关，高 CPS 节点慎开） -->
    <param name="publish-native-events" value="false"/>
    <!-- 事件附加通道变量白名单（逗号分隔） -->
    <param name="channel-params" value="hangup_cause,sip_from_user"/>
    <!-- 独立 CDR subject（默认 {prefix}event.cdr） -->
    <param name="cdr-subject" value=""/>
    <param name="workers" value="4"/>
    <param name="pub-qsize" value="8192"/>
    <param name="req-qsize" value="2048"/>
  </settings>
</configuration>
```

CLI：

```
fsctl> nats status    # 连接状态/收发计数/丢弃计数/会话数
fsctl> nats reload    # 重读配置
```

## 5. 编译

依赖：[nats.c](https://github.com/nats-io/nats.c)（官方 NATS C 客户端，>= 2.0，需启用 TLS 支持则加 `-DNATS_HAS_TLS` 构建）。本模块通过 pkg-config（`nats.pc`）发现依赖。

```bash
# 安装 libnats（Linux 示例）
cmake -B build -S . -DNATS_BUILD_WITH_TLS=OFF && cmake --build build && cmake --install build

# FreeSWITCH 侧
sed -i 's/#event_handlers\/mod_nats/event_handlers\/mod_nats/' build/modules.conf.in
./configure   # 重新生成（新增了 NATS pkg-config 检测，需先 autoreconf -i）
make mod_nats
make mod_nats-install
```

Windows：用 vcpkg（`vcpkg install nats.c`）或 CMake 自行构建 nats.c，工程文件待补充（.vcxproj）。

## 6. 快速联调

```bash
# 1. 起 NATS server
nats-server -p 4222

# 2. fs 里 load mod_nats，确认订阅建立
fs_cli -x "nats status"

# 3. 用 nats CLI 发一个 JStatus 请求
nats request 'nats.fs.node.<node_uuid>' \
  '{"jsonrpc":"2.0","id":"t1","method":"XNode.JStatus","params":{}}'

# 4. 订阅事件
nats sub 'nats.fs.event.channel.>'
nats sub 'nats.fs.event.cdr'
```

## 7. 设计决策记录

1. **RocketMQ 退役路线**：CDR/持久化数据改由 NATS JetStream 承接（在 nats-server 侧对 `event.cdr` 配 stream 即可，模块无需感知）；事件流保持 core NATS at-most-once。
2. **背压策略**：发布/请求双有界队列，满则丢弃并计数（`nats status` 可观测），绝不阻塞 FS core 线程。
3. **同名避让**：刻意不叫 mod_xcc（XSwitch 官方闭源模块名），协议兼容但实现独立，无 license 争议（协议定义 MIT/Apache 开源）。
4. **ESL 保持不动**：mod_event_socket 保留为运维通道（fs_cli），不参与新集成。

## 8. 已知限制（v0.1）

- XNode.Dial 的 Event.Result 目前不带原始 rpc id，用 `job_uuid` 关联；
- Accept 无 10 秒无人接管挂机逻辑（XCC 语义），来话需 dialplan 配合 park；
- 未经编译验证（需 libnats 环境），首次编译可能需修正个别 API 签名差异；
- Windows 工程（.vcxproj）未创建。

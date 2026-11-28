# of-ps

> 本地加固包含 303 个处理器、奖励、社交、装备、活动和支付安全状态机，详见[加固说明](docs/hardening-v3.0.md)。
> 公共大厅分支：`lab/public-lobby-phase0`，见 [PR #30](https://github.com/byzp/of-ps/pull/30)，尚未合并。

## 部署

1. 安装 `protoc`，编译协议：

   ```bash
   cd proto
   protoc net.proto --python_out .
   ```

2. 从[发布页](https://github.com/byzp/of-ps/releases)下载 `data.zip`，解压到 `resources`。不要使用其他来源资源。

3. 创建环境并安装依赖：

   ```bash
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install -r requirements-server.txt
   pip install -r requirements-client.txt
   ```

4. 启动服务：

   ```bash
   python main.py
   ```

5. 启动代理：

   ```bash
   mitmdump --mode local:launcher.exe,PCSDKUI.exe,QtWebEngineProcess.exe,ZFGameBrowser.exe --set connection_strategy=lazy -s .\Redirect.py
   ```

   浏览器访问 `mitm.it`，安装证书。

6. 启动游戏目录中的 `launcher.exe`。

## 公共大厅

### 当前实现

当前是**单进程中心化私服 + 在线状态投影 + 可选认证 KCP + 玩家节点 etcd 协调**，不是完整分布式副主机系统。

```text
客户端 → 登录 → TCP 会话 → 处理器 → 共享投影/场景记录
       → 全局快照 + 兴趣范围 → 普通/批量队列 → 客户端
另一台主机 ←签名、加密的可选 KCP→ 当前主机
玩家节点 etcd ←租约、事务、持久状态→ 当前频道主机
```

- 共享投影只保存场景、频道、位置、动作、表情等跨玩家状态。
- 每 0.5 秒发送频道完整快照；本地附近玩家高频更新，远端增量也经过兴趣范围。
- 普通/批量队列按 4:1 调度，但仍共享 TCP。
- 广播在锁内只生成消息快照，序列化和发送在锁外执行；动作按场景频道分片。
- 单文件快照使用跨进程锁和 epoch 比较；etcd 路径额外持久化名单、快照、事件和实体所有权。
- KCP 已接入 Ed25519 身份、nonce 防重放、信任白名单和 AES-GCM 加密；公网 NAT 尚未验收。

### etcd 协调与恢复

- 只启动显式配置的 etcd 二进制，不搜索 PATH、不下载、不回退内存实现；进程退出和健康检查失败都会失败关闭。
- 频道主机使用 lease 和 etcd 事务取得凭证，提交空操作后才能写状态。
- 应用任期单独持久化，不随主机租约删除；同主机刷新不增加任期，换主机后继续递增。
- 旧凭证使用主机 key 的全局单调 `mod_revision` 隔离。key 删除重建后 `version` 会重置，不能再用它防旧主机。
- 名单、快照、强模式事件和实体所有权不绑定旧主机租约；事件按稳定五元组幂等。
- 事件日志和名单复制器动态读取当前凭证，主机续租后不会继续使用旧 lease 或旧 `mod_revision`。
- 实体认领、签名转移和新领导接管使用 etcd CAS；新领导接管同时增加权威 epoch 和所有者代次。

### 目标架构

硬约束是不部署专用协调设施，但必须公网自动恢复。协调协议运行在每台玩家机器的网关中：

```text
游戏客户端 → 固定本地网关 → 当前频道主机
                │
                ├─ 玩家多数节点：成员、选主、世代编号、快照
                ├─ 签名写入隔离：拒绝旧主机
                └─ 玩家中继：直连失败时转发
```

- 创世邀请提供初始信任根和引导端点。
- 成员故障检测与节点同步负责发现和故障怀疑；玩家多数节点负责选主和元数据。
- 位置不进共识；聊天、动作、表情使用带序号的事件日志。
- 新主机由多数副本恢复；网关验证签名世代编号后重连。
- 私人数据不迁移；经济、支付和公共奖励不放玩家主机。

多数节点和网络路径可用时可以自动恢复；少数派分区会冻结。无公网可达节点时，对称 NAT/CGNAT 不能保证互通。

### 当前边界

| 项目 | 状态 |
|---|---|
| 全量回归 | `1527 passed，139 subtests passed`（2026-11-28） |
| 双协议客户端 | 登录、收到位置 ID、聊天、表情通过 |
| 双主机 KCP | 同机双进程签名加密跨服消息通过；协议机器人坐标级同步连续 5 轮通过 |
| 显式故障转移 | 强杀、人工提升、世代编号 1→3、重新登录通过 |
| 单主机防护 | 广播锁外化、动作分片、远端 AOI、位置限流、epoch 并发保护已实现 |
| 跨服安全 | Ed25519 协商与消息签名、nonce 防重放、信任白名单、AES-GCM 加密已接入 |
| 实体所有权 | 采集、怪物掉落、拾取已接入；etcd 持久 CAS、前任签名转移、新领导代次接管 |
| 在线名单与快照 | 版本化发布；新领导先取得凭证再恢复 etcd 快照，持久 key 不随旧租约删除 |
| 公共奖励 | 挑战/玩法箱、锦鲤、货币体力、礼物、领奖兑换、首通沙包、商店抽卡、制作餐厅和副本金币已加固；多人星级由服务端计时配置判定 |
| etcd 资源基线 | Windows etcd 3.6.6 空载：单节点最大工作集 29.1 MB；单机三节点合计约 104.5 MB；不是容量结论 |
| etcd 自动恢复 | 单机三进程强杀频道主机和 etcd1，约 5.56 秒取得第 2 任凭证；名单、快照、事件、实体恢复 |
| 旧凭证隔离 | 状态、名单、快照、事件、实体五类旧凭证写入全部拒绝；主机 key `mod_revision` 从 3 变为 11 |
| 真实客户端 | 同机两进程、固定端点、回调收到对方 ID、真实网络栈社交通过 |
| 未验收 | 跨物理节点、公网 etcd、坐标变化、移动输入、自动换主机、重连、数十客户端、长时稳定 |

合成 100 客户端替换了鉴权、持久化和完整处理器，只用于管线诊断。etcd 资源和故障恢复数据来自单机多进程，不代表跨主机故障域、公网延迟或生产负载。

### 下一步

1. 在三台物理节点上验证 etcd 网络、故障域、选主和恢复时间。
2. 将网关恢复状态接入启动器固定本地网关，验证客户端自动续接。
3. 用真实 STUN/TURN 验证公网 NAT、中继切换和配额。
4. 接入实体所有权自动迁移 producer，并明确持久事件保留与清理策略。
5. 完成真实客户端移动、退出、重启重连、数十客户端和公网长时验收。

## 公共大厅配置

| 配置 | 默认值 | 作用 |
|---|---:|---|
| `SCENE_GLOBAL_SYNC_INTERVAL` | `0.5` | 全局位置快照间隔 |
| `SCENE_AOI_RADIUS` | `80` | 附近玩家高频半径 |
| `LOBBY_STATE_PATH` | `None` | 快照路径，`None` 关闭 |
| `LOBBY_HOST_ID` | `local-host` | 主机标识 |
| `LOBBY_LEASE_SECONDS` | `10` | 进程内租约 |
| `LOBBY_SNAPSHOT_INTERVAL_SEC` | `1` | 快照间隔 |
| `LOBBY_RESTORED_PROJECTION_TTL_SEC` | `30` | 恢复投影过期时间 |
| `AUTO_GENERATE_PLAYER_NAME` | `True` | 自动命名，跳过新手登记 |
| `ALLOW_LOCAL_TOKEN` | `False` | 本机测试登录令牌白名单 |
| `CHAT_RATE_LIMIT_*` | 每秒 2，突发 5 | 聊天限流 |
| `ACTION_RATE_LIMIT_*` | 每秒 5，突发 10 | 动作限流 |
| `EMOJI_RATE_LIMIT_*` | 每秒 3，突发 8 | 表情限流 |
| `POSITION_RATE_LIMIT_*` | 每秒 30，突发 60 | 位置上行限流 |
| `OFPS_ETCD_ENDPOINT` | 未设置 | 启用 etcd 频道主机路径 |
| `OFPS_ETCD_EXECUTABLE` | 未设置 | 显式本机 etcd 二进制；缺失直接失败 |
| `OFPS_ETCD_CLIENT_URL` | `http://127.0.0.1:2379` | 伴生进程客户端地址 |
| `OFPS_ETCD_PEER_URL` | `http://127.0.0.1:2380` | 伴生进程节点间地址 |
| `OFPS_ETCD_INITIAL_CLUSTER` | 单节点 | 三节点部署时显式指定成员表 |
| `OFPS_ETCD_CLUSTER_TOKEN` | `ofps-etcd-cluster` | 初始集群标识；多节点必须显式设置唯一值 |
| `OFPS_LINK_IDENTITY_PATH` | `./link_identity.bin` | DPAPI 加密的跨服身份 |
| `OFPS_LINK_TRUSTED_NODE_IDS` | 空 | 跨服必须显式配置信任节点 ID |
| `OFPS_LINK_CHANNEL_ID` | `public` | 跨服握手绑定的频道 |

实体所有权记录使用 `OFPS_LINK_IDENTITY_PATH` 中的同一 Ed25519 身份签名。跨服场景启用 `OFPS_LINK_OTHER_SERVER=1` 时，采集、怪物死亡和拾取还要求配置 etcd；缺少共享所有权存储会失败关闭，不会退化为各主机自行认领。

## 验证

```bash
python -m pip install -r requirements-server.txt
python -m unittest discover -s tests -q
python -m pytest -q tests
python tools/lobby_e2e.py
python tools/lobby_failover_e2e.py
python tools/lobby_multihost_e2e.py
python tools/lobby_loadtest.py --help
python tools/etcd_resource_baseline.py --executable C:/etcd/etcd.exe --data-dir C:/ofps-etcd-baseline --mode cluster
python tools/etcd_channel_failover_acceptance.py --executable C:/etcd/etcd.exe --data-dir C:/ofps-etcd-failover --output C:/ofps-etcd-failover.json
```

- `lobby_e2e.py` 需要已启动且允许本机测试登录令牌的服务。
- `lobby_failover_e2e.py` 自行启动两个服务，强杀后显式提升替代主机。
- `lobby_multihost_e2e.py` 自行启动两个服务和 KCP。
- `etcd_resource_baseline.py` 只测显式二进制启动后的空载资源，不测玩家容量。
- `etcd_channel_failover_acceptance.py` 启动单机三进程 etcd，强杀频道主机和一个成员并验证恢复；不是跨物理节点验收。

## 控制台命令

| 命令 | 作用 |
|---|---|
| `give 玩家/全部 物品/全部 [数量]` | 发放物品 |
| `tp 玩家/全部 场景 [频道]` | 普通场景传送 |
| `tpd 玩家/全部 秘境` | 秘境传送 |
| `kick 玩家/全部` | 踢出玩家 |
| `players` | 在线玩家 |
| `link` | 已连接主机 |
| `metrics` | 联机指标 |
| `save` | 保存数据库 |
| `time 秒数` | 场景时间 |
| `firework 编号` | 烟花 |
| `stop` | 停止服务 |

## 可选 KCP

```bash
python utils/kcp/setup.py build_ext --inplace
```

频繁断线时可调整 `utils/kcp/ikcp.c` 或 `utils/kcp/_kcp.cpp` 的 `KCP_MINRTO` 后重新编译。

[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=Discord&logoColor=white)](https://discord.gg/FvsjdMMCY6)

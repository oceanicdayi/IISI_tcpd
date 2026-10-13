# go-tcpd-fast（TCPD Fast 地震速報服務）

`go-tcpd-fast` 是 Earthworm `tcpd_fast` 模組的 Go 語言 TCP 版本。服務透過 TCP 接收上游（`export_ew` / `export_gen_actv`）送來的測站 P 波 pick，在記憶體內完成事件觸發判定、震源定位與規模（Mpd / Mtc）估算，再將 EEW 速報以 Earthworm Link 訊框格式送往下游（`import_ew` / DCSN），同時輸出 `.rep` 詳細報告檔、分段日誌與 Prometheus 監控指標。

> 本文件依據專案原始碼撰寫（共約 3,650 行 Go 程式碼）。第 11 節「已知問題與待確認事項」列出靜態閱讀時發現的不一致處，尚未實際編譯或執行驗證。

> 程式架構、演算邏輯、觸發門檻、定位與規模計算、`.rep` 與日誌格式等的圖文說明網頁：[`tcpd_architecture.html`](tcpd_architecture.html)（單檔，可離線開啟）。網頁部署於 <https://github.com/oceanicdayi/IISI_tcpd>。

## 目錄

1. [系統架構與資料流](#1-系統架構與資料流)
2. [專案結構](#2-專案結構)
3. [核心處理流程（eewengine）](#3-核心處理流程eewengine)
4. [輸入與輸出格式](#4-輸入與輸出格式)
5. [設定檔參數](#5-設定檔參數)
6. [建置與執行](#6-建置與執行)
7. [日誌](#7-日誌)
8. [Prometheus 監控指標](#8-prometheus-監控指標)
9. [監控堆疊（docker-compose）](#9-監控堆疊docker-compose)
10. [測試](#10-測試)
11. [已知問題與待確認事項](#11-已知問題與待確認事項)
12. [相依套件](#12-相依套件)

---

## 1. 系統架構與資料流

```
上游 export_ew / export_gen_actv
        |  TCP :16001（Earthworm Link 訊框；Type 150 = pick，Type 3 = 心跳）
        v
+-------------------------------- tcpd-fast --------------------------------+
|  pickreceiver  --pickCh(容量256)-->  eewengine  -->  eewsender             |
|  (解析 14 欄位 pick)                 (過濾/觸發/定位/規模/報告)  (TCP client) |
|                                          |                                 |
|                                          +--> .rep 報告檔 (EEWOutputDir)    |
|  metrics (:8080/metrics)     Logger (service / network / event 三份日誌)    |
+---------------------------------------------------------------------------+
        |  TCP 127.0.0.1:16002（Earthworm Link 訊框；Type 150 = EEW，心跳）
        v
下游 import_ew (DCSN)
```

啟動順序（`cmd/tcpd-fast/main.go`）：

1. 解析命令列與 YAML 設定檔。
2. 建立三個 logrus logger（service / network / event）。
3. 啟動 Prometheus HTTP 伺服器（`metrics-addr`）。
4. 註冊 `SIGINT` / `SIGTERM`，收到訊號後取消 Context 進行優雅關機。
5. 建立 `pickCh`（容量 256）、啟動 `eewsender`（背景連線迴圈）、為每個 pick 監聽位址啟動一個 `pickreceiver`。
6. 啟動定時心跳（每 `heartbeat-interval` 秒送一次給下游）。
7. 執行 `eewengine.Run`（阻塞，直到 Context 結束）。

設計重點：

- 支援同時監聽多個位址／port（`pick-listen-addr` 與 `pick-listen-addrs` 合併並去重），對應多個測站網路各自送到不同 port 的情況。
- 本端**不**回送心跳給上游。`main.go` 註解說明：`export_gen_actv` 自行依 `SendAliveInt` 送心跳，若本端回送會被判定為 "weird heartbeat from client" 而自我終止重啟（已於實測確認）。`Receiver.WithHeartbeat` 仍保留但 `main` 未呼叫。
- 下游連線斷線時以指數退避（1 秒起，上限 60 秒）自動重連。

## 2. 專案結構

```
IISI_tcpd_source_code/
├── tcpd_fast.yaml                 # 設定檔副本（內容與 src/etc/tcpd_fast.yaml 完全相同）
└── TCPD_Source/
    ├── .env                       # GRAFANA_ADMIN_PASSWORD
    ├── .gitignore
    ├── docker-compose.yaml        # Prometheus / Loki / Promtail / Grafana
    ├── monitoring/
    │   ├── prometheus.yml         # 抓取 go-tcpd-fast:8080 與 prometheus 自身
    │   └── promtail-config.yaml   # 收集 service 日誌送往 Loki
    ├── pod/build/
    │   ├── Dockerfile             # alpine:3.19 映像
    │   └── createimage.sh         # 編譯 + podman build
    └── src/
        ├── go.mod / go.sum        # module: go-tcpd-fast（Go 1.23.1）
        ├── cmd/tcpd-fast/main.go  # 程式進入點
        ├── config/                # YAML 設定載入（config.go）
        ├── trigger/types.go       # 共用資料結構（PickEntry、Hypocenter、MagnitudeResult、SCNL…）
        ├── pickreceiver/          # TCP 監聽、Earthworm 訊框解包、pick 解析
        ├── eewengine/
        │   ├── engine.go          # 事件狀態機、觸發判定、報告發送、.rep 輸出
        │   ├── locaeq.go          # 震源定位（形心法 Locaeq、Geiger 疊代 LocaeqOri）
        │   └── magnitude.go       # 規模估算、PGA 預估
        ├── eewsender/             # 下游 TCP 連線、訊框封裝、心跳
        ├── metrics/               # Prometheus 指標與 HTTP 伺服器
        ├── Logger/                # logrus + lumberjack 日誌（含每日午夜滾動）
        └── etc/
            ├── tcpd_fast.yaml     # 目前使用的 YAML 設定
            └── tcpd_fast.d        # 舊版 Earthworm 格式設定（目前解析器無法讀取，見第 11 節）
```

各模組職責：

| 套件 | 職責 |
| --- | --- |
| `config` | 以 `gopkg.in/yaml.v3` 載入設定；檔案未出現的欄位保留 `DefaultConfig()` 預設值。 |
| `pickreceiver` | 每個監聽位址一個 `Receiver`；每條連線一個 goroutine，使用 `earthwormconn.EWLinkParser` 解包，只接受 `pick-type-id` 與 Type 3（心跳）；超過 `upstream-timeout` 無資料即關閉連線。 |
| `eewengine` | 唯一寫入 pick 緩衝區（最多 1000 筆）的 goroutine；每 100 ms 檢查事件逾時。 |
| `eewsender` | 維持對下游的 TCP 連線（撥號逾時 5 秒、KeepAlive 15 秒、寫入逾時 5 秒），送出 EEW 與心跳。 |
| `metrics` | 單例 Prometheus Registry，`/metrics` 端點，隨 Context 優雅關閉（逾時 5 秒）。 |
| `Logger` | 每個模組獨立日誌檔，10 MB 切檔、保留 30 份、90 天，另於本機時區每日午夜強制滾動。 |

## 3. 核心處理流程（eewengine）

### 3.1 Pick 進入（`processPick`）

1. 僅接受垂直分量：分量代碼第 3 碼必須為 `Z`（例如 `HHZ`、`HLZ`）。
2. 權重過濾：
   - 特殊測站（站名前綴 `YOJ`、`EOS2`、`EOS3`、`EOS4`）：`Weight >= 3` 即拒絕（寫死為 3）。
   - 其他測站：`Weight >= ignore-weight-p` 即拒絕。
3. 存入第一個空槽（`Flag == 0`），標記 `Flag = 1`，接著執行處理週期。

### 3.2 處理週期（`runProcessingCycle`）

1. **清除過期 pick**：`|現在時間 - P 到時| > active-parr-win` 者移除。
2. **合併重複測站**：同一 SCNL 且 P 到時相同的多筆 pick（代表同一測站在 P 波後第 N 秒的更新）只保留 `UpdSec` 較大者，並補入較小者的 `Pd`。
3. **計算形心**：所有有效 pick 的平均經度、緯度、P 到時。
4. **觸發測站篩選**：與形心距離超過 `trig-dis-win`（km）或與平均 P 時間差超過 `trig-tm-win`（秒）者剔除；但下列前綴測站一律保留：`EOS`、`YOJ`、`JMJ`、`PCY`、`LAY`、`TWH`、`PNG`、`PHU`、`PTM`、`PTT`、`VCH`、`VWU`、`WLC`。
5. **觸發條件**：篩選後測站數需 **≥ 4**（`<= 3` 時維持等待並將 `countMax` 歸零）。
6. **事件狀態管理**：
   - `countMax > report-limit`：視為新事件，重置狀態。
   - 測站數等於歷史最大值：不處理；小於歷史最大值：`countMax++` 並略過；大於歷史最大值：更新 `maxSta` 並進入定位／規模計算。
   - 該事件已發出超過 10 份報告（`repFin > 10`）後不再處理。

### 3.3 定位與規模（`processTrigger`）

| 階段 | 條件 | 演算法 | 品質檢查 |
| --- | --- | --- | --- |
| 快速模式 | 事件已發報告數 `repFin < 3` | `Locaeq`：形心法，震央取測站平均座標，深度固定 10 km，發震時間取平均 P 到時 − 2 秒 | 當最大震央距 `>= max-epi-dis` 時逐一剔除最遠測站（剩 < 4 站即放棄）；非共站測站數需 ≥ 4 |
| 標準模式 | `repFin >= 3` | `LocaeqOri`：10 次 Geiger 最小平方疊代 + 10～100 km（間隔 10 km）深度網格搜尋；速度模型為「分層 + 線性速度梯度」（P、S 各一組，詳見第 5 節） | 反覆剔除 \|Perr\| 最大測站，直到 `0.1 <= Averr <= 0.8` 且 `Avwei >= 0.3`（剩 < 5 站即放棄）；非共站測站數需 ≥ 5 |

兩種模式共通規則：

- **共站判定**：500 m 以內的測站視為共站，只計一站（`countNonCosite`）。
- 非共站測站數必須**大於上一次報告的測站數**才會再次發報（`gStaNum <= preGStaNum` 則略過）。
- 規模以 `XMpd` 與 `mag-min` / `mag-max` 比較，超出範圍則不發報（記錄 Warn）。

### 3.4 規模估算（`Magnitude`）

- **排除測站**：`mag-reject` 名單（SCNL 完全相符）、位置碼 `02`、非 Z 分量、`Tc < 0`、`UpdSec < 2`，這些測站 `Flag` 設為 2，不參與計算。
- **Mpd**（位移振幅規模），`R` 為震源距（km）、`Pd` 取 `Pd[min(UpdSec, 2)]`：

  | 儀器（`Inst`） | 公式 |
  | --- | --- |
  | HH（2） | `5.000 + 1.102·log10(Pd) + 1.737·log10(R)` |
  | HL（1） | `5.067 + 1.281·log10(Pd) + 1.760·log10(R)` |
  | HS（3） | `4.811 + 1.089·log10(Pd) + 1.738·log10(R)` |

- **Mtc**（特徵週期規模）：`4.218·log10(Tc) + 6.166`。
- 有效 Mpd 筆數需 > 3；以 Z-score < 1.0 過濾離群值後，取權重加權平均得 `XMpd`、`Mtc`。
- `ALLMag`：`Mtc` 與 `XMpd` 皆 > 0.001 時取平均，否則取 `XMpd`。**對外發報與範圍檢查使用的是 `XMpd`。**
- **Padj**（PGA 振幅調整係數）：以各站實測 `Pa` 與理論 `Pa`（HH/HL/HS 各有迴歸式）的比值，經 Z-score 過濾後加權平均。

### 3.5 報告發送（`reportSeq`）

- 每次報告 `repFin++`，事件編號 `numEew` 在 1～10000 循環。
- **前 3 份報告的早期發報閘門**（`ccount < 4`）：必須 `proTime < 10 s`；若 `pv-thr == 1` 且 `intensity-thr == 1`，另需 `EstimatePGA > 0`（22 個主要場址中預估 PGA ≥ 8 gal 的個數）且 `Q > 0`。未通過者不發送，但仍計入報告序號並輸出 `.rep` 檔。
- 報告數超過 `term-num` 後不再更新「前次」狀態（`preGStaNum` 等）。
- 若 `eew-output-dir` 非空，輸出 `.rep` 詳細報告。
- **事件重置**：第一份報告發出後超過 60 秒（`processTimeout`），清空所有 pick 與狀態並將 `numEew++`。

## 4. 輸入與輸出格式

### 4.1 Earthworm Link 訊框

```
[STX][InstID %3d][ModID %3d][TypeID %3d][body…][ETX]
```

body 內出現的 STX、ETX、ESC 位元組會加上 ESC 前綴逸出。收發兩端的 `buildEWFrame` 實作相同（`eewsender` 與 `pickreceiver` 各有一份）。

### 4.2 輸入：P 波 pick（Type = `pick-type-id`，預設 150）

body 為以空白分隔的 **14 欄位**文字，欄位數不符會記為解析失敗（`failed`）：

```
stn comp net loc lon lat Pa Pv Pd Tc P_epoch weight inst upd_sec
```

| # | 欄位 | 說明 |
| --- | --- | --- |
| 1–4 | `stn comp net loc` | 測站、分量（如 `HHZ`）、網路、位置碼（如 `01`） |
| 5–6 | `lon lat` | 經度、緯度（度） |
| 7 | `Pa` | P 波後峰值加速度 |
| 8 | `Pv` | P 波後峰值速度 |
| 9 | `Pd` | P 波後峰值位移，存入 `Pd[upd_sec]` |
| 10 | `Tc` | 特徵週期（秒） |
| 11 | `P_epoch` | P 波到時（Epoch 秒） |
| 12 | `weight` | 權重等級（數值愈小愈好） |
| 13 | `inst` | 儀器類型：1 = HL、2 = HH、3 = HS |
| 14 | `upd_sec` | 更新秒數（P 波後第幾秒的資料） |

範例（取自測試）：`TEST COMP NT LC 121.5 24.5 1.2 2.3 3.4 0.5 1600000000.0 1 2 3`

Type 3 訊框視為上游心跳，只更新連線活躍時間與健康度指標，不進入計算。

### 4.3 輸出：EEW 速報（Type = `eew-type-id`，預設 150）

```
%d %f %f %d Mpd %.1f %.2f %.2f %.1f %2d %2d %2d %.1f %.1f %d %d %.1f %c%c%c %.1f
```

整行共 19 個以空白分隔的欄位：

| # | 內容 | # | 內容 |
| --- | --- | --- | --- |
| 1 | 事件編號 `numEew` | 11 | 非共站測站數 `gStaNum` |
| 2 | 報告產生時間（Epoch 秒） | 12 | `n_mag`（目前固定為 0） |
| 3 | 發震時間（Epoch 秒） | 13 | 走時平均殘差 `averr` |
| 4 | 該事件第幾份報告 | 14 | 平均定位權重 `avwei` |
| 5 | 固定字串 `Mpd` | 15 | 品質指標 `Q`（`Pv > 0.257` 且位置碼 `01` 的測站數） |
| 6 | 規模 `XMpd` | 16 | 方位角空隙 `Gap`（度，取整數） |
| 7 | 震央緯度 | 17 | 處理時間 `proTime`（報告時間 − 發震時間，秒） |
| 8 | 震央經度 | 18 | 標記 `mark`（3 字元，不足補空白） |
| 9 | 震源深度（km） | 19 | `Padj` |
| 10 | 觸發測站數 `gN` | | |

### 4.4 輸出：`.rep` 報告檔

路徑：

```
<eew-output-dir>/event/<YYYY>/<MM>/<事件資料夾>/<發震時間 YYYYMMDD_HHMMSS>_n<報告序號>_<mark>.rep
```

- `<YYYY>/<MM>` 取自**報告產生當下的 UTC 時間**；檔名中的時間取自**發震時間（UTC）**。
- `<事件資料夾>` 為 `<第一份報告的 UTC 時間 YYYYMMDD_HHMMSS>_evt<事件編號>`，同一事件的後續報告沿用同一資料夾。
- 內容依序為：報告抬頭（averr、Q、Gap、Avg_wei、n、n_sta、Padj、no_eq）、發震時間與震源（lat、lon、dep、Mall、Mpd、Mtc、process_time）、各測站明細（pa、pv、pd、tc、Mtc、MPd、Perr、Dis、H_Wei、Parr、Pk_wei、Upd_sec、P_S、usd_sec）。
- 檔名與格式註解標示沿用 C 版規格。`Mpd_s`、`Mpv` 與各站 `MPv` 欄目前固定輸出 0。

## 5. 設定檔參數

設定檔為 **YAML**，預設路徑為執行目錄下的 `tcpd_fast.yaml`。載入規則：未出現於檔案的欄位使用下表「程式預設值」。

### 5.1 網路與識別

| 參數 | 程式預設值 | 隨附 YAML | 說明 |
| --- | --- | --- | --- |
| `metrics-addr` | `:8080` | （未設定） | Prometheus `/metrics` 監聽位址 |
| `pick-listen-addr` | `:16001` | `:16001` | 接收 pick 的 TCP 監聽位址（單一，向下相容） |
| `pick-listen-addrs` | （空） | （未設定） | 多個監聽位址清單，與上一項合併去重 |
| `eew-send-addr` | `127.0.0.1:16002` | `127.0.0.1:16002` | EEW 速報送出目標 |
| `install-id` / `module-id` | 1 / 8 | 1 / 8 | 訊框的 Installation / Module ID |
| `pick-type-id` / `eew-type-id` | 150 / 150 | 150 / 150 | pick 與 EEW 的訊息類型 ID |
| `upstream-timeout` | 30 | （未設定） | 上游無資料／心跳逾時秒數，逾時關閉連線 |
| `heartbeat-interval` | 15 | 15 | 對下游送出心跳的間隔（秒） |

### 5.2 觸發與報告

| 參數 | 程式預設值 | 隨附 YAML | 說明 |
| --- | --- | --- | --- |
| `trig-tm-win` | 8.0 | 40.0 | 觸發時間視窗（秒） |
| `trig-dis-win` | 60.0 | 180.0 | 觸發距離視窗（km） |
| `active-parr-win` | 60.0 | 300 | pick 存活時間（秒）；YAML 註解：為 tank-replay 測試加大，**正式環境為 45.0** |
| `report-limit` | 1 | 1 | 事件重置判斷門檻（`countMax` 超過即重置） |
| `term-num` | 15 | 15 | 報告數超過後不再更新前次狀態 |
| `max-epi-dis` | 100 | 100 | 快速模式下最大容許震央距（km） |
| `intensity-thr` / `pv-thr` | 1 / 0 | 1 / 1 | 兩者皆為 1 時，前 3 份報告啟用 PGA 預估閘門 |
| `ignore-weight-p` | 3 | 3 | 權重 ≥ 此值的 P pick 被忽略 |
| `mag-min` / `mag-max` | 0.5 / 10.0 | 0.5 / 10 | 可發報的規模範圍 |
| `mark` | `fst` | `fst` | 報告識別標記（輸出時補足 3 字元） |
| `mag-reject` | （空） | 20 筆 | 不參與規模估算的 SCNL 清單，格式 `{ sta, chn, net, loc }` |
| `eew-output-dir` | （空，不輸出 `.rep`） | `/mnt/d/VM/iisi/EEW/go-tcpd-fast/output` | `.rep` 輸出根目錄 |

### 5.3 速度模型

| 參數 | 說明 | 程式預設值 | 隨附 YAML |
| --- | --- | --- | --- |
| `boundary-p` | P 波淺／深層分界深度（km） | 35.0 | 40.0 |
| `sw-p-v` / `sw-p-vg` | 淺層 P 波速度（km/s）／梯度 | 4.5 / 0.085 | 5.10298 / 0.06659 |
| `dp-p-v` / `dp-p-vg` | 深層 P 波速度／梯度 | 6.0 / 0.023 | 7.80479 / 0.00457 |
| `boundary-s` | S 波淺／深層分界深度（km） | 35.0 | 50.0 |
| `sw-s-v` / `sw-s-vg` | 淺層 S 波速度／梯度 | 4.5 / 0.085 | 2.9105 / 0.0365 |
| `dp-s-v` / `dp-s-vg` | 深層 S 波速度／梯度 | 6.0 / 0.023 | 4.5374 / 0.0023 |

### 5.4 日誌

| 參數 | 程式預設值 | 說明 |
| --- | --- | --- |
| `log-path` | `logs` | 日誌根目錄 |
| `log-level` | `info` | `debug` / `info` / `warn` / `error` |
| `log-console` | `true` | 是否同時輸出到終端機 |

### 5.5 已解析但目前未被程式邏輯使用的欄位

`log-file`、`show-report`、`ignore-weight-s`、`my-module-id`、`ring-name`、`ring-name-out`（檢查方式：搜尋原始碼中對這些欄位的引用，除設定結構定義與預設值外皆無使用）。

## 6. 建置與執行

### 6.1 本機

```bash
cd TCPD_Source/src
go build -o tcpd-fast ./cmd/tcpd-fast/

./tcpd-fast                      # 讀取 ./tcpd_fast.yaml
./tcpd-fast -c /path/to/tcpd_fast.yaml
./tcpd-fast /path/to/tcpd_fast.yaml   # 也可用第一個位置參數指定設定檔
```

> `go.mod` 以 `replace` 指向 `../go-earthworm-connect`（私有模組 `git1.midev/eqc-pws/go-earthworm-connect`），該目錄**不在**本專案資料夾內，編譯前需將其放置於 `TCPD_Source/go-earthworm-connect`。

### 6.2 容器映像

```bash
bash TCPD_Source/pod/build/createimage.sh
```

腳本會以 `CGO_ENABLED=0 GOOS=linux go build` 編譯到 `pod/build/bin/tcpd-fast`（基底映像為 alpine/musl，必須關閉 cgo），再以 `podman build -t eew-tcpd:latest` 建立映像。

映像內容：`alpine:3.19`、時區 `Asia/Taipei`、執行檔 `/opt/eew-tcpd/bin/tcpd-fast`、工作目錄 `/opt/eew-tcpd`、對外埠 `16001`（pick）與 `8080`（metrics）。映像內**未**含設定檔，執行時需將設定檔掛載到 `/opt/eew-tcpd/tcpd_fast.yaml`（預設設定檔路徑為工作目錄下的 `tcpd_fast.yaml`），或以 `-c` 指定。

執行範例（請依實際環境調整路徑與埠）：

```bash
podman run -d --name eew-tcpd \
  -p 16001:16001 -p 8080:8080 \
  -v /path/to/tcpd_fast.yaml:/opt/eew-tcpd/tcpd_fast.yaml:ro \
  -v /path/to/logs:/opt/eew-tcpd/logs \
  -v /path/to/output:/path/in/yaml/eew-output-dir \
  eew-tcpd:latest
```

## 7. 日誌

依 TCPD 分段日誌規格拆成三個檔案，位於 `<log-path>/<module>/`：

| 檔案 | 內容 | 寫入者 |
| --- | --- | --- |
| `service/tcpd_service.log` | 服務生命週期（設定載入、啟停、關機） | `main` |
| `network/tcpd_network.log` | 連線、重連、逾時、心跳 | `eewsender`、`pickreceiver`（連線層） |
| `event/tcpd_event.log` | pick 解包、觸發、定位、規模、發報、`.rep` 輸出 | `eewengine`、`pickreceiver`（pick 解包） |

- 行格式：`2006-01-02 15:04:05.000 [level] message`，訊息慣用 `(模組::動作)` 標籤，例如 `(Engine::Publish)`、`(EEWSender::Reconnect)`。
- 切檔：單檔 10 MB、保留 30 份、90 天，使用本機時間；另於每日午夜（本機時區）強制 `Rotate`，舊內容改名為帶時間戳的備份檔。

## 8. Prometheus 監控指標

端點：`http://<host>:8080/metrics`（可由 `metrics-addr` 調整）。

| 指標 | 類型 | 說明 |
| --- | --- | --- |
| `eew_pick_receiver_connections_active` | Gauge | 目前上游連線數 |
| `tcpd_pickreceiver_upstream_healthy` | Gauge | 上游健康狀態（1 正常 / 0 逾時） |
| `tcpd_pickreceiver_upstream_last_active_timestamp` | Gauge | 最後收到訊息或心跳的 Unix 時間 |
| `eew_pick_receiver_parsed_total{status}` | Counter | pick 解析次數，`status` 為 `success` / `failed` |
| `eew_pick_receiver_heartbeats_sent_total` | Counter | 回送給上游的心跳數（目前流程不啟用） |
| `eew_sender_connected` | Gauge | 與下游連線狀態（1 / 0） |
| `eew_sender_reconnects_total` | Counter | 重連嘗試次數 |
| `eew_sender_reports_total` | Counter | 已送出的 EEW 報告數 |
| `eew_sender_heartbeats_total` | Counter | 已送出的心跳數 |
| `eew_engine_process_duration_seconds` | Histogram | 單筆 pick 處理耗時 |
| `eew_engine_pro_time_seconds{report_num}` | Histogram | 發震時間到報告輸出的時間差，依報告序號分組 |
| `eew_engine_channel_depth` | Gauge | `pickCh` 緩衝區佇列深度 |

## 9. 監控堆疊（docker-compose）

`TCPD_Source/docker-compose.yaml` 啟動以下服務：

| 服務 | 映像 | 埠 | 備註 |
| --- | --- | --- | --- |
| prometheus | `prom/prometheus:v2.45.0` | 9090 | 每 5 秒抓取 `host.containers.internal:8080` |
| loki | `grafana/loki:2.8.2` | 3100 | 使用映像內建設定 |
| promtail | `grafana/promtail:2.8.2` | （內部 9080） | 掃描 `/var/log/go-tcpd-fast/service/*.log`，解析時間戳（`Asia/Taipei`）與 `level` 標籤 |
| grafana | `grafana/grafana:9.5.2` | 3000 | 管理員密碼取自 `.env` 的 `GRAFANA_ADMIN_PASSWORD`（預設 `admin`） |

```bash
cd TCPD_Source
podman-compose up -d      # 或 docker compose up -d
```

> 注意：compose 檔掛載的是 `./config/prometheus.yml` 與 `./config/promtail-config.yaml`，但實際檔案位於 `monitoring/`，請見第 11 節。

## 10. 測試

```bash
cd TCPD_Source/src
go test ./...
```

| 測試檔 | 涵蓋內容 |
| --- | --- |
| `config/config_test.go` | 設定解析與預設值 |
| `eewengine/locaeq_test.go` | `Delaz`、`Matinv`、`Locaeq`、`LocaeqOri`、`Wt` |
| `eewengine/magnitude_test.go` | `CalTc`、`Mpd1HH/HL/HS`、`PaHH`、`CalZ`、`CalAvgStdMag`、`Magnitude`、`EstimatePGA` |
| `eewsender/sender_test.go` | 收送與心跳、未連線錯誤、寫入逾時與指標、panic 復原 |
| `metrics/metrics_test.go` | 單例初始化、HTTP 伺服器啟停 |
| `pickreceiver/receiver_test.go` | 連線數與解析指標、心跳處理、上游逾時 |

`eewengine/engine.go`（事件狀態機、觸發與發報邏輯）目前沒有對應的單元測試。第 11 節說明目前部分測試與程式碼不同步。

## 11. 已知問題與待確認事項

以下為靜態閱讀原始碼與設定檔的結果，**尚未實際編譯或執行**；建議於補齊相依模組後逐項確認。

### 建置與測試

1. **私有相依缺失**：`go.mod` 的 `replace git1.midev/eqc-pws/go-earthworm-connect => ../go-earthworm-connect` 指向的目錄不在目前專案資料夾內，無法在此狀態下編譯。
2. **`go.mod` 未直接列出 `gopkg.in/yaml.v3`**：`config/config.go` 已匯入該套件，但 `go.mod` 只列出 `go.yaml.in/yaml/v2`（indirect），`yaml.v3` 僅出現在 `go.sum`。建議執行 `go mod tidy` 確認。
3. **`pickreceiver/receiver_test.go` 無法編譯**：測試以 `New(addr, 19, 30, logger)`（4 個參數）呼叫，現行簽章為 `New(listenAddr, pickTypeID, upstreamTimeout, networkLogger, eventLogger)`（5 個參數）。
4. **`config/config_test.go` 與現行解析器不符**：`TestParseFile` 的測試內容仍是舊版 `.d` 鍵值格式，而 `ParseFile` 現只解析 YAML，預期此測試會失敗。
5. **`src/etc/tcpd_fast.d`**：舊版 Earthworm 格式設定檔，目前的 YAML 解析器無法讀取（`main.go` 的 `./tcpd-fast my_config.d` 範例註解亦已過時）。

### 部署與設定

6. **docker-compose 掛載路徑不符**：`./config/prometheus.yml`、`./config/promtail-config.yaml` 在專案中實際位於 `./monitoring/`。
7. **Promtail 只收 `service` 日誌**：`__path__` 為 `.../service/*.log`，`network`、`event` 兩份日誌（含發報紀錄）不會進入 Loki。
8. **隨附設定為測試值**：`active-parr-win: 300`（正式建議 45.0）、`eew-output-dir` 為 WSL 風格路徑 `/mnt/d/VM/...`；`.env` 的 Grafana 密碼為預設 `admin`。
9. **兩份相同的 `tcpd_fast.yaml`**（專案根目錄與 `src/etc/`）需自行維持同步。

### 程式行為（建議對照需求確認）

10. **未使用的設定欄位**：`log-file`、`show-report`、`ignore-weight-s`、`my-module-id`、`ring-name`、`ring-name-out`（見 5.5）。
11. **寫死於程式碼的規則**：特殊測站權重門檻（`YOJ`、`EOS2/3/4` 固定 `>= 3`）、觸發時保留的測站前綴清單、最少測站數（觸發 ≥ 4、標準模式 ≥ 5）、事件 60 秒逾時、報告數上限 10、`Averr` 0.1～0.8 / `Avwei` ≥ 0.3 判斷、共站距離 0.5 km、22 個 PGA 場址係數表。若需求要求可調整，需改為設定項。
12. **`LocaeqOri` 的剔除條件**：迴圈條件為 `!(Averr <= 0.8 && Averr >= 0.1 && Avwei >= 0.3)`，因此 `Averr < 0.1`（極佳擬合）時也會持續剔除測站，請確認是否與 C 版行為一致。
13. **`parsePick` 邊界**：`Pd` 陣列長度為 15，以 `upd_sec` 為索引；`upd_sec` 若為負數或 ≥ 15 會觸發 index out of range，`handleConn` 的 `recover` 雖可攔截，但該連線會被關閉。另外數值欄位的轉換錯誤被忽略（視為 0）。
14. **輸出欄位暫為固定值**：EEW 訊息的 `n_mag`（原始碼註解 "Will be set properly"）、`.rep` 的 `Mpd_s`、`Mpv`、各站 `MPv` 皆為 0。
15. **緩衝區滿載**：`ptr` 槽位 1000 筆全滿時，新 pick 會被靜默丟棄（無日誌或指標）。
16. **多監聽位址共用健康指標**：`tcpd_pickreceiver_upstream_healthy` 為全域單一 Gauge，任一連線逾時都會將其設為 0，即使其他連線仍正常。
17. **程式碼重複**：`buildEWFrame` 在 `eewsender` 與 `pickreceiver` 各有一份相同實作。

## 12. 相依套件

| 套件 | 版本 | 用途 |
| --- | --- | --- |
| `git1.midev/eqc-pws/go-earthworm-connect` | `v0.0.0`（本機 replace） | Earthworm Link 訊框解析與常數 |
| `github.com/prometheus/client_golang` | v1.23.2 | 監控指標 |
| `github.com/sirupsen/logrus` | v1.9.3 | 日誌 |
| `gopkg.in/natefinch/lumberjack.v2` | v2.2.1 | 日誌切檔 |
| `gopkg.in/yaml.v3` | （`go.mod` 未列出，見第 11 節） | YAML 設定解析 |

Go 版本：`go 1.23.1`。

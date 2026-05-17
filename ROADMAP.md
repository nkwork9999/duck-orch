# duckOrch ROADMAP — Phase 11+ (Asset 本格化 + MCP)

作成日: 2026-05-17
ステータス: 設計確定、未着手

[DESIGN.md](DESIGN.md) は Phase 0-9 の完了済み機能を記録するドキュメント。
本ファイルは Phase 11 以降の **Asset 中心アーキテクチャへの転換** と **MCP サーバ追加** の設計。

---

## 背景: なぜ今この方向か

### 業界トレンド (2026-05 時点)
- **Airflow 3** が Asset-Aware Scheduling を正式採用 (旧 Datasets → Assets 改名)
- **Dagster** が Declarative Automation を標準化 (`AutomationCondition.eager / on_cron / on_missing`)
- **Snowflake Dynamic Tables** が `TARGET_LAG` 宣言だけで自動再計算する世界を実現
- **dbt Fusion** が State-Aware Orchestration を Preview で投入
- 共通方向: **「時刻で起動」から「データ/イベントで起動」へ**

### 加えて Agent 連携の流れ
- MCP (Model Context Protocol) サーバが 200+ 実装、Anthropic / Snowflake / Databricks 各社採用
- Claude Code SDK → Claude Agent SDK 改名 (2026 初頭)
- Snowflake と Anthropic が $200M 提携、Cortex Code が顧客 50% 利用

### duckOrch の立ち位置
- 大手クラウドと**競合しない**: ローカル / オフライン / 個人〜小チーム向けに特化
- 売り: **「飛行機の中で動く Dynamic Tables + Asset orchestrator」**
- 思想: **Dagster Asset の柔軟性 + Snowflake Dynamic Tables の宣言の簡潔さ + DuckDB の軽さ**

---

## 設計の柱

### 柱 1: Asset を一級市民化
タスクが outputs[] を持つ「半 Asset」状態から、Asset を独立した最上位概念に格上げ。
タスクは「Asset を materialize する手段」になる。

### 柱 2: Snowflake `TARGET_LAG` 構文を取り込む
Dagster の `AutomationCondition` は表現力が高いが冗長。
Snowflake の `TARGET_LAG = '5 minutes'` は**1 行で freshness 宣言**できる。
両方サポートし、`@target_lag` を `automation = eager() throttle 5min` に内部展開。

### 柱 3: CLI / SQL pragma / MCP の 3 経路カバレッジ
すべての機能を 3 つの経路から呼べる:
- **CLI**: `duck-orch ...` (ターミナル / シェルスクリプト / cron)
- **SQL pragma**: `CALL orch_xxx()` (DuckDB セッション内)
- **MCP**: Claude Code / 任意の MCP クライアントから

### 柱 4: Jinja 風構文の新規拡張を停止
- 既存 `crates/orch_runtime/src/templating.rs` の `{{ var }}` 置換コードは**後方互換のため残す**
- **新規変数** (Partition 等) は DuckDB ネイティブ `$param` バインドで実装
- DSL 拡張による複雑化を避ける

---

## ブランチ運用

```
main
 ├── feature/mcp       ← Phase 11 のみ (独立、先行 merge 可)
 └── feature/asset     ← Phase 12〜17 (連続コミット、内部で小 PR)
```

Phase 11 (MCP) と Phase 12 以降 (Asset) は完全独立で並行開発可能。

---

## Phase 一覧

| Phase | 内容 | 工数 | ブランチ |
|---|---|---|---|
| 11 | MCP サーバ (軽量) | 1〜2 日 | feature/mcp |
| 12 | 新規構文を DuckDB ネイティブ `$param` に確定 | 0.5 日 | feature/asset |
| 13 | Asset 一級化 | 3〜4 日 | feature/asset |
| 14 | Partition (Dagster 由来) | 5〜7 日 | feature/asset |
| 15 | AutomationCondition + TARGET_LAG (Dagster + Snowflake 融合) | 4〜5 日 | feature/asset |
| 16 | Freshness / Asset Check / SLA | 2〜3 日 | feature/asset |
| 17 | `CREATE DYNAMIC ASSET` SQL 構文 (Snowflake 互換層) | 3 日 | feature/asset |
| **合計** | | **18.5〜24.5 日** | |

---

## Phase 11: MCP サーバ (軽量)

### 目的
Claude Code から duckOrch を直接叩ける状態にする。

### 実装
- 新クレート `crates/orch_mcp/`
- stdio transport (Claude Code 標準)
- 既存 CLI の薄いラッパー (CLI が既に `--json` 全対応)
- Rust 単体、Python サイドカー不要

### 公開ツール (最小 8 個)

| ツール | 種別 | 内訳 |
|---|---|---|
| `list_pipelines` | 読み | `__orch__.tasks` 集約 |
| `list_assets` | 読み | `__orch__.assets` (Phase 13 で実データ流入) |
| `list_runs` | 読み | `__orch__.runs` 直近 N 件 |
| `describe_task` | 読み | 1 タスク詳細 |
| `get_lineage` | 読み | Mermaid 文字列 |
| `impact` | 読み | 下流影響 |
| `validate` | 読み | 構文・参照チェック |
| `run_pipeline` | 書き | デフォルト `dry_run=true` |

### 安全策
- 書き込み系は `dry_run=true` デフォルト
- 失敗時は `error_context_json` を必ず添える (Phase 9 で実装済)
- 認証なし (ローカル DuckDB ファイル前提)

### 着地条件
- `~/.claude.json` の MCP 設定 1 行で繋がる
- `mcp inspect` でツール一覧確認可
- README に Claude Code 統合手順

---

## Phase 12: 構文移行 (Jinja → DuckDB ネイティブ)

### 方針
- **既存の `{{ var }}` コードは触らない** (後方互換維持)
- **新規変数** (Phase 13 以降) は `$param` バインドで実装

### 新規実装
- `crates/orch_runtime/src/binding.rs` (新規)
  - DuckDB `PREPARE` + 名前付きバインド
  - 型付きパラメータ (`STRING / INT / DATE / TIMESTAMP`)
  - ヘッダ `-- @param partition_key:DATE` パース
- `${ident}` 単純置換 (識別子用、bind 不能な箇所のみ)

### 既存
- `crates/orch_runtime/src/templating.rs` はそのまま (deprecated 化はしない)
- 既存タスクファイル (`tasks/example/*.sql`) は変更不要

### 工数
0.5 日

---

## Phase 13: Asset 一級化

### 新スキーマ

```sql
CREATE TABLE __orch__.assets (
    name VARCHAR PRIMARY KEY,        -- 例: 'analytics.user_stats'
    kind VARCHAR,                    -- 'table' | 'view' | 'external' | 'file' | 'model'
    location VARCHAR,                -- 物理位置
    group_name VARCHAR,              -- UI/CLI でのグルーピング
    owner VARCHAR,
    description VARCHAR,
    code_version VARCHAR,            -- 定義 SQL のハッシュ (変更検知)
    defined_by_task VARCHAR,         -- どの task が生む
    tags VARCHAR[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE __orch__.asset_materializations (
    asset_name VARCHAR,
    partition_key VARCHAR DEFAULT '__default__',
    materialized_at TIMESTAMP,
    run_id UUID,
    rows BIGINT,
    bytes BIGINT,
    status VARCHAR,                  -- 'success' | 'failed' | 'in_progress'
    PRIMARY KEY (asset_name, partition_key, materialized_at)
);

CREATE TABLE __orch__.asset_edges (
    upstream_asset VARCHAR,
    downstream_asset VARCHAR,
    via_task VARCHAR,
    edge_type VARCHAR,               -- 'direct' | 'aggregated' | 'derived'
    PRIMARY KEY (upstream_asset, downstream_asset, via_task)
);
```

### タスクヘッダ拡張

```sql
-- @asset name=analytics.user_stats
-- @asset_kind table
-- @asset_group sales
-- @description 国別アクティブユーザー数
-- @owner data-team@example.com
```

既存の `@outputs` がある場合は**自動で Asset 登録** (後方互換)。

### CLI

```bash
duck-orch asset list                       # 全 asset 一覧
duck-orch asset list --group sales --json
duck-orch asset show <name>                # 詳細
duck-orch asset lineage <name>             # Mermaid
duck-orch asset materializations <name>    # 履歴
duck-orch asset health                     # 健全性サマリ (Phase 16 で拡充)
```

### SQL pragma

```sql
CALL orch_asset_list();
CALL orch_asset_show('analytics.user_stats');
SELECT * FROM orch_asset_lineage('analytics.user_stats');
SELECT * FROM orch_asset_materializations('analytics.user_stats');
```

### MCP 自動拡張
Phase 11 の `list_assets / describe_asset` 等が実データを返すようになる (コード変更不要)。

---

## Phase 14: Partition (最重要)

### ヘッダ

```sql
-- @partitions_by daily(start=2026-01-01)
-- @partitions_by static(jp,us,eu)
-- @partitions_by multi(date=daily(start=2026-01-01), region=static(jp,us,eu))
-- @param partition_key:DATE
```

### SQL 内参照 (Phase 12 の `$param` を使用)

```sql
-- @asset name=analytics.daily_orders
-- @partitions_by daily(start=2026-01-01)
-- @param partition_key:DATE

CREATE OR REPLACE TABLE analytics.daily_orders AS
SELECT * FROM raw.orders
WHERE order_date = $partition_key;
```

Multi-dim の場合:

```sql
-- @param partition_date:DATE
-- @param partition_region:STRING

SELECT * FROM raw.orders
WHERE order_date = $partition_date
  AND region = $partition_region;
```

### 新スキーマ

```sql
CREATE TABLE __orch__.asset_partitions (
    asset_name VARCHAR,
    partition_key VARCHAR,
    dimension_values JSON,           -- {"date":"2026-05-17","region":"jp"}
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_name, partition_key)
);
```

### CLI

```bash
duck-orch backfill <asset> --from 2026-01-01 --to 2026-04-30 [--parallel N]
duck-orch backfill <asset> --partition 2026-05-17       # 1 partition だけ
duck-orch backfill <asset> --missing                    # 欠落 partition だけ
duck-orch asset partitions <asset>                      # カレンダー風 ASCII
duck-orch asset partitions <asset> --json
```

### SQL pragma

```sql
CALL orch_backfill('analytics.daily_orders', '2026-01-01', '2026-04-30');
SELECT * FROM orch_asset_partitions('analytics.daily_orders');
```

### 出力イメージ

```
analytics.daily_orders  (DailyPartition, start=2026-01-01)
  2026-05-13  ✅   2026-05-14  ✅   2026-05-15  ✅
  2026-05-16  🟡   2026-05-17  ⚪   2026-05-18  ⚪

Status: 3 success, 1 in_progress, 2 missing
Backfill: duck-orch backfill analytics.daily_orders --missing
```

---

## Phase 15: AutomationCondition + TARGET_LAG (核心)

### Dagster 系構文

```sql
-- @automation eager
-- @automation on_cron("0 6 * * *")
-- @automation on_missing
-- @automation eager AND NOT in_progress
```

### Snowflake Dynamic Tables 系構文

```sql
-- @target_lag 5min
```

`@target_lag 5min` は内部的に `automation = eager() throttle 5min` に展開される。
ユーザにとっては Snowflake 風の宣言、内部的には Dagster 風の評価器。

### サポートする AutomationCondition (MVP)

| 名前 | 意味 |
|---|---|
| `eager()` | いずれかの上流 asset が前回 materialize 以降に更新されたら |
| `on_cron(expr)` | cron 時刻以降に上流が更新されたら (時刻ぴったりではない) |
| `on_missing()` | partition が未 materialize なら |
| `freshness_violated()` | freshness policy 違反なら |
| `in_progress()` | 現在実行中なら (NOT と組み合わせる用) |

### Automation Sensor

- 既存 Phase 8 scheduler thread を拡張
- 30 秒ごとに全 Asset の condition 評価 (可変間隔、`orch_sensor_interval_seconds` で調整可)
- 条件成立 → run キューに投入
- 同時実行ポリシー (`@schedule_concurrency = skip|queue|overlap`) と統合

### スキーマ

```sql
ALTER TABLE __orch__.assets ADD COLUMN automation_condition VARCHAR;
ALTER TABLE __orch__.assets ADD COLUMN target_lag_seconds INT;

CREATE TABLE __orch__.automation_evaluations (
    asset_name VARCHAR,
    evaluated_at TIMESTAMP,
    condition_met BOOLEAN,
    reason TEXT,
    PRIMARY KEY (asset_name, evaluated_at)
);
```

### CLI

```bash
duck-orch automation status                # 全 asset の condition 評価
duck-orch automation simulate <asset>      # 「いま走るか?」dry-run
duck-orch sensor enable
duck-orch sensor disable
duck-orch sensor status
```

### SQL pragma

```sql
CALL orch_sensor_start();
CALL orch_sensor_stop();
SELECT * FROM orch_automation_status();
SELECT * FROM orch_automation_simulate('analytics.daily_orders');
```

---

## Phase 16: Freshness / Asset Check / SLA

### Freshness Policy

```sql
-- @freshness max_lag=60min
```

違反時の動作:
- `duck-orch asset health` で警告表示
- OpenLineage に `slaMiss` facet を emit
- Phase 15 の `freshness_violated()` condition で再 materialize trigger

### Asset Check (旧 `@test` の昇格)

```sql
-- @check name=no_nulls "SELECT COUNT(*) FROM ${asset} WHERE x IS NULL" expect 0
-- @check name=positive_revenue "SELECT MIN(rev) FROM ${asset}" expect_gt 0
-- @check_severity error    # check 失敗で downstream をブロック
-- @check_severity warn     # 警告のみ、downstream は走る
```

`${asset}` は「自分の asset 名」展開 (識別子のため `${}` 形式)。

### スキーマ

```sql
CREATE TABLE __orch__.asset_checks (
    asset_name VARCHAR,
    check_name VARCHAR,
    sql VARCHAR,
    expect_type VARCHAR,         -- 'eq' | 'gt' | 'lt' | 'between'
    expect_value VARCHAR,
    severity VARCHAR,            -- 'error' | 'warn'
    PRIMARY KEY (asset_name, check_name)
);

CREATE TABLE __orch__.asset_check_results (
    asset_name VARCHAR,
    check_name VARCHAR,
    run_id UUID,
    executed_at TIMESTAMP,
    status VARCHAR,              -- 'pass' | 'fail'
    actual_value VARCHAR,
    PRIMARY KEY (asset_name, check_name, executed_at)
);
```

### CLI

```bash
duck-orch asset health [--json]
duck-orch check run <asset>
duck-orch check history <asset>
```

---

## Phase 17: `CREATE DYNAMIC ASSET` SQL 構文 (Snowflake 互換)

### 新 DuckDB SQL 構文

```sql
CREATE DYNAMIC ASSET analytics.user_stats
  TARGET_LAG = '5 minutes'
  AS
  SELECT country, COUNT(*) AS users
  FROM analytics.clean_users
  GROUP BY country;
```

### 内部展開
- `__orch__.assets` に登録
- `automation_condition = 'eager() throttle 5min'`
- Automation Sensor が拾って自動 materialize
- **Snowflake `CREATE DYNAMIC TABLE` 構文と互換**

### CLI

```bash
duck-orch dynamic list
duck-orch dynamic refresh <asset>      # 強制再計算
duck-orch dynamic migrate-from-snowflake <sql-dump-file>
```

### 移行ヘルパー
- Snowflake の `SHOW DYNAMIC TABLES` 出力を食って duckOrch 用 SQL を生成
- 「Snowflake で書いた Dynamic Table がそのままローカル DuckDB で動く」が売り

---

## CLI / SQL / MCP の 3 経路カバレッジ表

duckOrch の差別化は**「3 経路すべてで同じ事ができる」**こと。

| 機能 | CLI | SQL pragma | MCP |
|---|---|---|---|
| パイプライン登録 | `register` | `CALL orch_register()` | `register_task` |
| 実行 | `run` | `CALL orch_run()` | `run_pipeline` |
| 状態 | `status` | `SELECT * FROM orch_status()` | `list_runs` |
| Asset 一覧 | `asset list` | `CALL orch_asset_list()` | `list_assets` |
| Asset 詳細 | `asset show <n>` | `CALL orch_asset_show()` | `describe_asset` |
| Partition 一覧 | `asset partitions` | `orch_asset_partitions()` | `list_partitions` |
| Backfill | `backfill` | `CALL orch_backfill()` | `backfill_asset` |
| Lineage | `lineage` | `orch_lineage()` | `get_lineage` |
| Impact | `impact` | `orch_impact()` | `impact` |
| Health | `asset health` | `orch_asset_health()` | `asset_health` |
| Check 実行 | `check run` | `CALL orch_check_run()` | `run_check` |
| Automation 評価 | `automation status` | `orch_automation_status()` | `automation_status` |
| Sensor 制御 | `sensor enable` | `CALL orch_sensor_start()` | `enable_sensor` |
| Dynamic 作成 | (SQL のみ) | `CREATE DYNAMIC ASSET ...` | `create_dynamic` |
| Dynamic refresh | `dynamic refresh` | `CALL orch_dynamic_refresh()` | `refresh_dynamic` |

3 種類のユーザ全員が同じ機能にアクセスできる:
- ターミナルで叩く人
- DuckDB CLI で SQL を書く人
- Claude に喋らせる人

---

## 設計上の重要判断 (確定事項)

1. **既存 `@outputs` の扱い**: 自動で Asset 化する (後方互換)
2. **Asset の Location 範囲**: MVP は同じ DuckDB ファイル内、外部 (S3/外部DB) は Phase 18+
3. **Partition の物理表現**: メタデータのみ (テーブル分割しない、WHERE で論理 partition)
4. **Automation Sensor のポーリング間隔**: 30 秒デフォルト、`orch_sensor_interval_seconds` で可変
5. **MCP transport**: MVP は stdio のみ (Claude Code 標準)、HTTP は Phase 18+
6. **Jinja 風構文 (`{{}}`)**: 既存コードは残す (後方互換)、新規機能では `$param` バインドを使う

---

## 着手順 (推奨)

1. **Phase 11 (MCP) を `feature/mcp` で 1〜2 日**
   - 即効性高い、Asset 改修中も Claude 経由で duckOrch を触れる
2. **Phase 12 (構文確定) を `feature/asset` で 0.5 日**
3. **Phase 13 → 14 → 15 → 16 → 17** を `feature/asset` で連続
   - Phase 13 完了時点で Phase 11 の MCP `list_assets` 等が実データを返す (相乗効果)

---

## 最終的なポジショニング (Zenn / chemdatatravelers 用)

> **「Snowflake の Dynamic Tables 思想 + Dagster の Asset 思想を、DuckDB ネイティブ・オフライン・1 ファイル完結で実現する。CLI / SQL / Claude のどこからでも同じ機能を使える」**

- Snowflake と土俵が違う (クラウド/エンタープライズ vs ローカル/個人) → 競合しない
- Dagster と土俵が違う (Python/ヘビー vs DuckDB 拡張/軽量) → 競合しない
- ニッチ: **「飛行機の中で動く Dynamic Tables」**

---

## 参考にしたソース

- Airflow 3 Asset-Aware Scheduling: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
- Dagster Declarative Automation: https://docs.dagster.io/guides/automate/declarative-automation
- Dagster Software-Defined Assets: https://dagster.io/blog/software-defined-assets
- Snowflake Dynamic Tables: https://docs.snowflake.com/en/user-guide/dynamic-tables-about
- dbt Fusion: https://docs.getdbt.com/docs/fusion/about-fusion
- Anthropic MCP: https://github.com/modelcontextprotocol
- Dagster MCP Server: https://dagster.io/blog/dagsters-mcp-server

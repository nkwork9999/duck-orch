# duckOrch

**DAG オーケストレーション + 副次リネージ** を 1 つの DuckDB 拡張に詰めたもの。
タスクを書くと依存解決・並列実行・Mermaid 可視化・OpenLineage 発行まで全部出る。

```sql
LOAD duckorch;
PRAGMA orch_register('./tasks/');   -- *.sql を読み込み
PRAGMA orch_run;                    -- DAG 順に実行
PRAGMA orch_visualize('lineage');   -- Mermaid を取り出す
```

```bash
duck-orch register ./tasks/
duck-orch run --json
duck-orch graph > pipeline.md       # GitHub PR でそのまま見える
```

---

## できること

| | 内容 |
|---|---|
| **タスク定義** | 1 SQL ファイル = 1 タスク。先頭に `-- @key value` でメタ情報(SQLMesh 風) |
| **依存解決** | `inputs`/`outputs` から DAG 自動構築。書かなくても `sqlparser-rs` が SQL から推測 |
| **実行** | トポロジカル順、レイヤごとに並列(`SET orch_max_parallel = 4`) |
| **失敗対応** | exponential backoff リトライ → ダメなら downstream を skip |
| **インクリメンタル** | `@incremental_by ts` + `{{ last_processed_at }}` で差分処理 |
| **テスト** | `@test "SQL" expect 0` で実行後アサーション |
| **可視化** | Mermaid (lineage / dag / combined) を `PRAGMA orch_visualize` で取得 |
| **観測** | OpenLineage 互換イベントを Marquez / DataHub に POST |
| **スケジュール** | `duck-orch schedule add NAME "0 6 * * *"` + `daemon` で常駐 |
| **AI連携** | 全 CLI に `--json`、`validate` で構造化エラー、`impact` で影響範囲 |

リネージは「タスクの inputs/outputs から自動派生する副産物」という位置づけで、メイン機能は **DAG オーケストレーション** です。

---

## クイックスタート

### 1. ビルド

```bash
git clone --recurse-submodules https://github.com/nkwork9999/duckorch.git
cd duckorch
make                              # DuckDB 同梱でフルビルド (10〜30分)
cargo build -p duckorch_cli --release  # CLI バイナリ
```

成果物:
- `build/release/duckdb` — duckorch をロード済みの DuckDB CLI
- `build/release/extension/duckorch/duckorch.duckdb_extension` — 拡張本体
- `target/release/duck-orch` — 単体 CLI

### 2. タスクを書く

```sql
-- tasks/clean_users.sql
-- @task name=clean_users
-- @outputs analytics.clean_users
-- @retries 2

CREATE OR REPLACE TABLE analytics.clean_users AS
SELECT id, name, country FROM raw.users WHERE deleted_at IS NULL;
```

```sql
-- tasks/user_stats.sql
-- @task name=user_stats
-- @outputs analytics.user_stats
-- @test "SELECT COUNT(*) FROM analytics.user_stats WHERE users < 0" expect 0

CREATE OR REPLACE TABLE analytics.user_stats AS
SELECT country, COUNT(*) AS users
FROM analytics.clean_users
GROUP BY country;
```

`inputs` は SQL から自動抽出されるので書かなくて OK。

### 3. 動かす

```bash
duck-orch register ./tasks/
duck-orch run
duck-orch graph
```

または DuckDB の中から:

```sql
LOAD duckorch;
PRAGMA orch_register('./tasks/');
PRAGMA orch_run;
SELECT task_name, status FROM __orch__.runs ORDER BY started_at;
```

---

## タスクファイル仕様

```sql
-- @task name=user_stats               必須(または `-- @name user_stats`)
-- @description 国別アクティブユーザー数
-- @owner data-team@example.com
-- @inputs analytics.clean_users       省略可(SQL から自動抽出)
-- @outputs analytics.user_stats        必須(または SQL から自動抽出)
-- @depends_on clean_users              省略可(inputs から推測)
-- @schedule "0 6 * * *"                cron(5フィールド)
-- @retries 3
-- @timeout 300                         秒
-- @incremental_by updated_at           インクリメンタル列
-- @tags daily, analytics
-- @test "SELECT COUNT(*) FROM x WHERE y < 0" expect 0

<SQL本体>
```

### Jinja 風プレースホルダ(インクリメンタル時)

```sql
INSERT INTO log
SELECT * FROM raw.events
WHERE event_time > {{ last_processed_at }}
  AND event_time <= {{ now }};
```

サポート変数: `{{ last_processed_at }}`, `{{ now }}`, `{{ run_id }}`

---

## CLI

```
duck-orch [--db <path>] [--ext <path>] <subcommand> [--json]

  register <dir>           タスクファイル一括ロード
  run                      DAG 実行
  status                   直近 run 履歴
  graph [lineage|dag|combined]   Mermaid 出力
  test                     @test を実行
  validate <file>          1ファイル検証(構造化 JSON 返却)
  impact <table>           このテーブルを変更すると何が壊れる?
  lineage <table>          上流データ系譜
  schedule add <name> <cron>     cron 登録
  schedule daemon          常駐ループ(30秒ごとに run-due)
```

`--json` を付けると Claude / エージェントが parse できる JSON で出力。

---

## SQL API

```sql
LOAD duckorch;

-- 設定
SET orch_max_parallel = 4;
SET orch_openlineage_url = 'http://marquez:5000/api/v1/lineage';
SET orch_openlineage_debug = true;

-- 操作
PRAGMA orch_init;                       -- __orch__ スキーマ作成
PRAGMA orch_register('./tasks/');       -- ディレクトリから読み込み
PRAGMA orch_run;                        -- 実行
PRAGMA orch_test;                       -- @test 走らせる
PRAGMA orch_visualize('lineage');       -- Mermaid

-- スカラー関数(純粋変換)
SELECT orch_extract_io('INSERT INTO x SELECT * FROM y');
-- {"inputs":["y"],"outputs":["x"]}
SELECT orch_render_mermaid(dag_json, 0, '[]');

-- 状態テーブル
SELECT * FROM __orch__.tasks;
SELECT * FROM __orch__.runs WHERE status = 'failed';
SELECT * FROM __orch__.lineage_edges;
```

---

## アーキテクチャ

「**C++ 薄皮 + Rust 本体**」のサンドイッチ構成。

```
┌─ C++ 拡張 (~700 行) ─────────────────────┐
│  PRAGMA / Scalar function 登録            │
│  Connection で SQL 実行 (per-thread)      │
│  std::thread で並列ディスパッチ          │
└──────────────┬───────────────────────────┘
               ↕ extern "C" FFI
┌─ Rust workspace ─────────────────────────┐
│  orch_common   Task / FFI helpers        │
│  orch_dag      DAG / topo / Mermaid      │
│  orch_lineage  sqlparser-rs              │
│  orch_runtime  Parser / Templating       │
│  orch_ol       OpenLineage HTTP worker   │
│  orch_core     extern "C" facade         │
│  orch_cli      duck-orch バイナリ        │
└──────────────────────────────────────────┘
```

DuckDB の C API には optimizer フックが未公開なので、Rust 単体では「クエリへの自動割り込み」ができない。C++ 薄皮側で必要な DuckDB 機能を呼び、ロジックは全部 Rust に逃がす。
ducksmiles と同じパターンです。

詳細は [DESIGN.md](DESIGN.md) 参照。

---

## ステータス

実装は Phase 1〜9 がすべて完了。Phase 2(optimizer フック経由のクエリ自動傍受)のみ延期(明示登録 + sqlparser-rs 自動抽出で代替可)。

| Phase | 内容 | 状態 |
|---|---|---|
| 0 | プロジェクト骨組み | ✅ |
| 1 | パーサ + DAG + 実行 | ✅ |
| 2 | optimizer フック | ⏸ |
| 3 | Mermaid | ✅ |
| 4 | リトライ + skip | ✅ |
| 5 | 並列実行 | ✅ |
| 6 | CLI | ✅ |
| 7 | インクリメンタル + test | ✅ |
| 8 | スケジューラ | ✅ |
| 9 | OpenLineage | ✅ |

---

## 開発

```bash
cargo test --workspace --release    # Rust ユニットテスト
make                                # DuckDB ごとフルビルド
make debug                          # デバッグビルド

# Rust だけ素早く回す
cargo build -p duckorch_core --release
cargo build -p duckorch_cli --release
```

DuckDB / extension-ci-tools は `v1.5.1` で固定(submodule)。バージョンを上げる場合は両方揃えて差し替え。

---

## ライセンス

MIT。詳細は [LICENSE](LICENSE)。

## クレジット

duckOrch のコードはすべて新規執筆ですが、設計上のアイデアは下記プロジェクトを参考にしました(コードコピーはありません):

- [ducksmiles](https://github.com/duckdb/community-extensions/tree/main/extensions/ducksmiles) — C++ + Rust ハイブリッドのビルド構成
- [duck_lineage](https://github.com/ilum-cloud/duck_lineage) — DuckDB から OpenLineage を発行するアイデア
- [SQLMesh](https://sqlmesh.readthedocs.io) — タスクファイルのコメントヘッダ形式
- [dbt](https://docs.getdbt.com) — テスト形式・downstream skip・`--full-refresh`
- [OpenLineage](https://openlineage.io) — イベント仕様(Apache-2.0、互換性は仕様準拠)

duckOrch は「タスク実行が主、リネージは inputs/outputs から派生する副産物」という設計思想で、duck_lineage(任意クエリの傍受リネージ)とは補完関係です。両方ロードして併用できます。

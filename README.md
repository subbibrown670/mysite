# mysite

## 動作環境

- Python 3.13 以上
- uv（Python のパッケージ管理ツール）

---

## セットアップ手順

### 1. uv をインストールする

ターミナルを開いて、以下のコマンドを実行してください。

**Mac の場合:**
```bash
brew install uv
```

**Windows / Mac / Linux（共通）:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

インストールできたか確認します。バージョン番号が表示されれば OK です。
```bash
uv --version
```

### 2. 必要なライブラリをインストールする

以下のコマンドを実行すると、仮想環境の作成と必要なライブラリのインストールが自動で行われます。

```bash
uv sync
```

---

## スクリプトの実行方法

### 食中毒情報を収集する

```bash
uv run python scripts/collect_food_poisoning.py
```

### 営業停止情報を収集する

```bash
uv run python scripts/collect_suspensions.py
```

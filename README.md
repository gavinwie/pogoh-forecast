# pogoh-forecast

Live bike availability forecasting for Pittsburgh's POGOH bike share (GBFS feed).
This repo currently contains **Phase 1: the data collector**. It snapshots
`station_status` every ~10 minutes via GitHub Actions and stores small Parquet
files on a dedicated `data` branch.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -q

# Hit the real feed once and write a snapshot locally:
PYTHONPATH=src python -m bikecast.collect --out local_data
```

(On Windows PowerShell: `$env:PYTHONPATH="src"; python -m bikecast.collect --out local_data`.)

A successful run prints a JSON report (rows kept, duplicates dropped, feed age,
stale flag) and writes `local_data/raw/<date>/<time>.parquet`.

If discovery fails, open the feed URL in a browser, check the real path, and pass
it with `--url`; or update `DEFAULT_GBFS_URLS` in `src/bikecast/gbfs.py`.

## Deploy the collector (free)

1. Create a **public** GitHub repo and push this code to `main`.
2. Create the data branch once:
   ```bash
   git checkout --orphan data
   git rm -rf .
   git commit --allow-empty -m "init data branch"
   git push origin data
   git checkout main
   ```
3. In the repo: **Settings > Actions > General > Workflow permissions** -> *Read and write permissions*.
4. **Actions** tab -> *collect* -> **Run workflow**. Confirm a `snapshot ...` commit appears on the `data` branch.
5. The schedule (`*/10 * * * *`) takes over from there.

## Design notes

- **Gaps are normal.** GitHub cron is best-effort; expect missed runs. Downstream code must treat time as irregular.
- **Stale feed flag.** `feed_last_updated` is stored with every row and `feed_stale` is set when it lags the fetch time by more than 15 minutes. Nothing is silently fixed.
- **GBFS v2 and v3** are both parsed (v3 renamed fields and changed timestamp/name formats).
- **One small file per snapshot**, written atomically. A nightly compaction job (next phase) merges them.

## Layout

```
src/bikecast/gbfs.py       fetch + parse GBFS v2/v3
src/bikecast/validate.py   dedupe, invalid-row drop, staleness
src/bikecast/collect.py    CLI entry point, Parquet writer
tests/                     pytest suite (fixtures, no network)
.github/workflows/         collect (cron) and ci
```

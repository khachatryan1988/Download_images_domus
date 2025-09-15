
```bash
docker compose build
docker compose run --rm downloader --dry-run --max 50
docker compose run --rm downloader --concurrency 16 --fast
```

Results appear in `./images/<item_id>/<basename>.webp`.

### Filtering
```
--item ER-00043311 --item ABC-123
--id 42
```

### Performance modes
- `--fast`: fewer paths, shorter timeouts (skips rare variants)
- `--ultra`: minimal checks, fastest, higher chance of misses

### Throttling
- `--rps 5`
- `--delay 0.2`

### Progress
- `--progress auto|tqdm|simple|none`

### Environment variables (DB)
- `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_TABLE_PRODUCTS`

## Local run (Python)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python download_images.py --fast --concurrency 12```## N- Verifies image, compresses to WebP (binary search on quality), and may downscale if needed

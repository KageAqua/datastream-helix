name: Fetch Market Data

on:
  schedule:
    - cron: '20 9 * * 1-5'
    - cron: '50 3 * * 1-5'
  workflow_dispatch:

jobs:
  fetch:
    runs-on: ubuntu-latest
    env:
      FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install requests pandas "xlrd==1.2.0" openpyxl lxml beautifulsoup4

      - name: Run fetch script
        run: python scripts/fetch_data.py 2>&1 | tee fetch_log.txt

      - name: Show log
        if: always()
        run: cat fetch_log.txt

      - name: Upload log as artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: fetch-log
          path: fetch_log.txt

      - name: Commit and push data
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          git diff --staged --quiet || git commit -m "chore: update market data $(date -u +'%Y-%m-%d %H:%M UTC')"
          git push

<details>
<summary>Source troubleshooting</summary>

- If a chart is empty, verify that `evidence/sources/site_data/current/` contains the latest export.
- If schemas changed, run `make check-site` to refresh Evidence sources and rebuild.
- The site never connects to ClickHouse directly; it reads committed flat files only.

</details>

---
title: About
---

{@partial "site_nav.md"}

# About

Short methodology and freshness notes for the published GitHub Pulse snapshot.

```sql freshness
select *
from site_data.site_freshness
limit 1
```

## Freshness

<BigValue data={freshness} value="days_since_snapshot" title="Days Since Snapshot" />
<BigValue data={freshness} value="hours_since_generated" title="Hours Since Build" />

## Methodology

The site is built from public repository metadata only. Data is collected in batch, transformed with dbt, exported as static files, and published without live warehouse access at runtime.

---
title: Owners
---

{@partial "site_nav.md"}

# Owners

Owner-level summary for the current published snapshot.

```sql owner_summary
select
  owner_group,
  repo_count,
  total_stars,
  avg_stars,
  total_forks,
  total_watchers,
  active_repo_count,
  archived_repo_count,
  language_count
from site_data.owner_summary
order by total_stars desc, repo_count desc
limit 30
```

<BarChart
  data={owner_summary}
  x="owner_group"
  y="total_stars"
  title="Stars by Owner"
/>

<DataTable data={owner_summary} />

{@partial "source_support.md"}

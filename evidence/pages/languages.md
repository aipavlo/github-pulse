---
title: Languages
---

{@partial "site_nav.md"}

# Languages

Language distribution across published repositories.

```sql language_summary
select
  language,
  repo_count,
  total_stars,
  avg_stars,
  median_stars,
  total_forks,
  stale_repo_count
from site_data.language_summary
order by total_stars desc, repo_count desc
limit 25
```

<BarChart
  data={language_summary}
  x="language"
  y="total_stars"
  title="Stars by Language"
/>

<DataTable data={language_summary} />

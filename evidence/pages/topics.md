---
title: Topics
---

{@partial "site_nav.md"}

# Topics

Topic-level summary for the current published snapshot.

```sql topic_summary
select
  topic,
  repo_count,
  repo_uniq_count,
  total_stars,
  avg_stars,
  median_stars,
  total_forks,
  total_watchers
from site_data.topic_summary
order by total_stars desc, repo_uniq_count desc
limit 30
```

<BarChart
  data={topic_summary}
  x="topic"
  y="total_stars"
  title="Stars by Topic"
/>

<DataTable data={topic_summary} />

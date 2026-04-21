---
title: Repositories
---

{@partial "site_nav.md"}

# Repositories

Top repositories from the public publish dataset.

```sql repos
select
  popularity_rank,
  repo_full_name,
  owner_login,
  repo_url,
  primary_language,
  stargazers_count,
  forks_count,
  watchers_count,
  repo_popularity_score,
  is_archived,
  is_fork
from site_data.repo_top
order by popularity_rank
limit 50
```

```sql stars_by_repo
select
  repo_full_name,
  stargazers_count
from site_data.repo_top
order by stargazers_count desc
limit 15
```

<BarChart
  data={stars_by_repo}
  x="repo_full_name"
  y="stargazers_count"
  title="Top Repositories by Stars"
/>

<DataTable data={repos} />

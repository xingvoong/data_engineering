-- Composite health score per repo based on activity, responsiveness, and contribution rate.

with repos as (
    select * from {{ ref('stg_repos') }}
),

issues as (
    select
        repo_full_name,
        count(*)                                      as total_issues,
        sum(case when is_closed then 1 else 0 end)    as closed_issues,
        avg(days_to_close)                            as avg_days_to_close
    from {{ ref('stg_issues') }}
    group by repo_full_name
),

prs as (
    select
        repo_full_name,
        count(*)                                      as total_prs,
        sum(case when is_merged then 1 else 0 end)    as merged_prs,
        avg(days_to_merge)                            as avg_days_to_merge,
        avg(total_changes)                            as avg_pr_size
    from {{ ref('stg_pull_requests') }}
    group by repo_full_name
),

combined as (
    select
        r.repo_id,
        r.repo_full_name,
        r.repo_name,
        r.description,
        r.primary_language,
        r.stars,
        r.forks,
        r.open_issues,
        r.last_pushed_at,
        r.created_at,

        -- issue metrics
        coalesce(i.total_issues, 0)                                       as total_issues,
        coalesce(i.closed_issues, 0)                                      as closed_issues,
        round(coalesce(i.closed_issues, 0)::float
            / nullif(i.total_issues, 0) * 100, 1)                        as issue_close_rate_pct,
        round(coalesce(i.avg_days_to_close, 0), 1)                       as avg_issue_close_days,

        -- PR metrics
        coalesce(p.total_prs, 0)                                          as total_prs,
        coalesce(p.merged_prs, 0)                                         as merged_prs,
        round(coalesce(p.merged_prs, 0)::float
            / nullif(p.total_prs, 0) * 100, 1)                           as pr_merge_rate_pct,
        round(coalesce(p.avg_days_to_merge, 0), 1)                       as avg_pr_merge_days,

        -- staleness
        date_diff('day', r.last_pushed_at, current_timestamp)            as days_since_last_push,

        -- health score (0–100): weighted composite
        -- 30% stars (log-scaled), 25% issue close rate, 25% pr merge rate, 20% recency
        round(
            least(30, log(greatest(r.stars, 1)) / log(50000) * 30)
            + coalesce(i.closed_issues, 0)::float / nullif(i.total_issues, 0) * 25
            + coalesce(p.merged_prs, 0)::float / nullif(p.total_prs, 0) * 25
            + greatest(0, 20 - date_diff('day', r.last_pushed_at, current_timestamp) / 18.25)
        , 1)                                                               as health_score

    from repos r
    left join issues i on r.repo_full_name = i.repo_full_name
    left join prs p    on r.repo_full_name = p.repo_full_name
)

select * from combined
order by health_score desc

-- Issue resolution funnel and SLA tracking per repo.

with issues as (
    select * from {{ ref('stg_issues') }}
),

stats as (
    select
        repo_full_name,

        count(*)                                                     as total_issues,
        sum(case when is_closed then 1 else 0 end)                   as closed_issues,
        sum(case when not is_closed then 1 else 0 end)               as open_issues,

        -- close rate
        round(
            sum(case when is_closed then 1 else 0 end)::float
            / nullif(count(*), 0) * 100, 1
        )                                                            as close_rate_pct,

        -- resolution time percentiles (closed issues only)
        round(avg(days_to_close), 1)                                 as avg_days_to_close,
        round(median(days_to_close), 1)                              as median_days_to_close,
        round(percentile_cont(0.9) within group
            (order by days_to_close), 1)                             as p90_days_to_close,

        -- SLA buckets (% closed within N days)
        round(
            sum(case when days_to_close <= 1 then 1 else 0 end)::float
            / nullif(sum(case when is_closed then 1 else 0 end), 0) * 100, 1
        )                                                            as pct_closed_within_1d,
        round(
            sum(case when days_to_close <= 7 then 1 else 0 end)::float
            / nullif(sum(case when is_closed then 1 else 0 end), 0) * 100, 1
        )                                                            as pct_closed_within_7d,
        round(
            sum(case when days_to_close <= 30 then 1 else 0 end)::float
            / nullif(sum(case when is_closed then 1 else 0 end), 0) * 100, 1
        )                                                            as pct_closed_within_30d,

        -- volume over time
        sum(case when created_at >= current_timestamp - interval '30 days'
            then 1 else 0 end)                                       as issues_last_30d,
        sum(case when created_at >= current_timestamp - interval '7 days'
            then 1 else 0 end)                                       as issues_last_7d

    from issues
    group by repo_full_name
)

select * from stats
order by total_issues desc

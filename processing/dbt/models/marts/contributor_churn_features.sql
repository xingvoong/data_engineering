-- Temporal feature snapshot for contributor churn prediction.
-- One row per (developer, repo_full_name), anchored to current_date as snapshot_date.
-- Feast reads this via a parquet export from DuckDB.
--
-- Churn risk score (0.0–1.0) weights:
--   50% recency  — inactivity is the strongest open-source churn signal
--   35% frequency decline — PR rate drop in the most recent 90-day window vs prior
--   15% engagement decline — comment rate on recent issues vs a healthy baseline

with pr_windows as (
    select
        author_login,
        repo_full_name,
        max(created_at)                                                           as last_pr_at,
        count(*) filter (
            where created_at >= current_timestamp - interval '90 days'
        )                                                                         as pr_count_last_90d,
        count(*) filter (
            where created_at >= current_timestamp - interval '180 days'
              and created_at <  current_timestamp - interval '90 days'
        )                                                                         as pr_count_prior_90d,
        sum(case when is_merged then 1 else 0 end)                                as total_prs_merged
    from {{ ref('stg_pull_requests') }}
    where author_login is not null
    group by author_login, repo_full_name
),

issue_windows as (
    select
        author_login,
        repo_full_name,
        max(created_at)                                                           as last_issue_at,
        avg(comments) filter (
            where created_at >= current_timestamp - interval '90 days'
        )                                                                         as comment_rate_last_90d
    from {{ ref('stg_issues') }}
    where author_login is not null
    group by author_login, repo_full_name
),

combined as (
    select
        coalesce(p.author_login, i.author_login)       as developer,
        coalesce(p.repo_full_name, i.repo_full_name)   as repo_full_name,
        current_date                                    as snapshot_date,

        -- last-seen timestamps
        p.last_pr_at,
        i.last_issue_at,
        greatest(p.last_pr_at, i.last_issue_at)        as last_contribution_at,

        -- recency (null when no activity in that dimension)
        case when p.last_pr_at is not null
            then date_diff('day', p.last_pr_at, current_timestamp)
        end                                             as days_since_last_pr,

        case when i.last_issue_at is not null
            then date_diff('day', i.last_issue_at, current_timestamp)
        end                                             as days_since_last_issue,

        -- overall recency: use the most recent touch across both dimensions
        case when greatest(p.last_pr_at, i.last_issue_at) is not null
            then date_diff('day', greatest(p.last_pr_at, i.last_issue_at), current_timestamp)
        end                                             as days_since_last_contribution,

        -- PR frequency windows
        coalesce(p.pr_count_last_90d, 0)               as pr_count_last_90d,
        coalesce(p.pr_count_prior_90d, 0)              as pr_count_prior_90d,
        round(coalesce(p.pr_count_last_90d,  0) / 90.0, 4) as pr_frequency_last_90d,
        round(coalesce(p.pr_count_prior_90d, 0) / 90.0, 4) as pr_frequency_prior_90d,

        -- engagement
        round(coalesce(i.comment_rate_last_90d, 0.0), 4) as comment_rate_last_90d,

        -- all-time
        coalesce(p.total_prs_merged, 0)                as total_prs_merged

    from pr_windows p
    full outer join issue_windows i
        on p.author_login  = i.author_login
       and p.repo_full_name = i.repo_full_name
),

scored as (
    select
        *,

        -- recency component: 0.0 (active today) → 1.0 (90+ days silent)
        round(
            least(coalesce(days_since_last_contribution, 90), 90) / 90.0
        , 4)                                            as recency_component,

        -- frequency decline component: 0.0 (no decline) → 1.0 (dropped to zero)
        -- only fires when the contributor had prior-period activity
        round(
            case
                when pr_frequency_prior_90d > 0
                then greatest(0.0, 1.0 - (pr_frequency_last_90d / pr_frequency_prior_90d))
                else 0.0
            end
        , 4)                                            as frequency_component,

        -- engagement component: 0.0 (≥2 comments/issue) → 1.0 (no engagement)
        -- baseline of 2.0 comments per issue is treated as "healthy"
        round(
            greatest(0.0, 1.0 - least(comment_rate_last_90d, 2.0) / 2.0)
        , 4)                                            as engagement_component

    from combined
)

select
    developer,
    repo_full_name,
    snapshot_date,
    last_pr_at,
    last_issue_at,
    last_contribution_at,
    days_since_last_pr,
    days_since_last_issue,
    days_since_last_contribution,
    pr_count_last_90d,
    pr_count_prior_90d,
    pr_frequency_last_90d,
    pr_frequency_prior_90d,
    comment_rate_last_90d,
    total_prs_merged,
    recency_component,
    frequency_component,
    engagement_component,

    -- weighted churn risk score
    round(
        0.50 * recency_component
        + 0.35 * frequency_component
        + 0.15 * engagement_component
    , 3)                                                as churn_risk_score

from scored
order by churn_risk_score desc

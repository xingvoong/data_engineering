-- Per-developer activity summary across issues and PRs.

with issue_authors as (
    select
        author_login,
        repo_full_name,
        count(*)                                    as issues_opened,
        sum(case when is_closed then 1 else 0 end)  as issues_closed
    from {{ ref('stg_issues') }}
    where author_login is not null
    group by author_login, repo_full_name
),

pr_authors as (
    select
        author_login,
        repo_full_name,
        count(*)                                    as prs_opened,
        sum(case when is_merged then 1 else 0 end)  as prs_merged,
        avg(days_to_merge)                          as avg_days_to_merge
    from {{ ref('stg_pull_requests') }}
    where author_login is not null
    group by author_login, repo_full_name
),

combined as (
    select
        coalesce(ia.author_login, pa.author_login)     as developer,
        coalesce(ia.repo_full_name, pa.repo_full_name) as repo_full_name,

        coalesce(ia.issues_opened, 0)                  as issues_opened,
        coalesce(ia.issues_closed, 0)                  as issues_closed,
        coalesce(pa.prs_opened, 0)                     as prs_opened,
        coalesce(pa.prs_merged, 0)                     as prs_merged,
        round(coalesce(pa.avg_days_to_merge, 0), 1)    as avg_days_to_merge,

        -- total contribution score
        coalesce(ia.issues_opened, 0)
        + coalesce(pa.prs_merged, 0) * 3              as contribution_score

    from issue_authors ia
    full outer join pr_authors pa
        on ia.author_login = pa.author_login
        and ia.repo_full_name = pa.repo_full_name
)

select * from combined
order by contribution_score desc

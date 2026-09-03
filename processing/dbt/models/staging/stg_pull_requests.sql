with source as (
    select * from {{ source('github_raw', 'pull_requests') }}
),

renamed as (
    select
        id::varchar           as pr_id,
        number                as pr_number,
        repo_full_name,
        title,
        state,
        author_login,
        is_merged,
        base_branch,
        head_branch,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        closed_at::timestamp  as closed_at,
        merged_at::timestamp  as merged_at,

        -- derived
        case when merged_at is not null
            then date_diff('day', created_at::timestamp, merged_at::timestamp)
        end                   as days_to_merge
    from source
)

select * from renamed

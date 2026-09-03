with source as (
    select * from {{ source('github_raw', 'repositories') }}
),

renamed as (
    select
        id::varchar          as repo_id,
        full_name            as repo_full_name,
        name                 as repo_name,
        description,
        html_url,
        stars,
        forks,
        open_issues,
        primary_language,
        owner_login,
        owner_type,
        is_fork,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        pushed_at::timestamp  as last_pushed_at
    from source
)

select * from renamed

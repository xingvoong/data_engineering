with source as (
    select * from {{ source('github_raw', 'issues') }}
),

renamed as (
    select
        id::varchar           as issue_id,
        number                as issue_number,
        repo_full_name,
        title,
        state,
        author_login,
        comments,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        closed_at::timestamp  as closed_at,

        -- derived
        case when closed_at is not null
            then date_diff('day', created_at::timestamp, closed_at::timestamp)
        end                   as days_to_close,

        state = 'closed'      as is_closed
    from source
)

select * from renamed

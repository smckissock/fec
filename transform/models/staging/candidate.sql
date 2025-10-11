-- models/normalized/candidate.sql
select * from {{ source('raw', 'candidate') }}
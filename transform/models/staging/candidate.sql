-- models/staging/candidate.sql
select * from {{ source('raw', 'candidate') }}
with airports as (
    select
        "Airport_Code" as airport_code,
        "Airport_Name" as airport_name,
        "Airport_Employees" as airport_employees,
        "Airport_Size" as airport_size 
    
    from {{ source('aircraft_analytics', 'airports')}}
)
select * from airports
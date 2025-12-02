with aircrafts as (
    select
        "Aircraft_Id" as aircraft_id,
        "Aircraft_Type" as aircraft_type,
        "Mass" as mass,
        "Length" as lenght,
        "Cost" as cost,
        "Capacity" as Capacity
    from {{ source('aircraft_analytics', 'aircraft')}}
)
select * from aircrafts
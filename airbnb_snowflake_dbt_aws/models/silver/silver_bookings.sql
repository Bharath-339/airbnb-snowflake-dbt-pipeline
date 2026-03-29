{{
    config(
        materialized='incremental',
        unique_key='BOOKING_ID'
    )
}}
select
    BOOKING_ID,
    LISTING_ID,
    BOOKING_DATE,
    {{ multiply('BOOKING_AMOUNT', 'NIGHTS_BOOKED', 2) }} as TOTAL_AMOUNT,
    CLEANING_FEE,
    SERVICE_FEE,
    BOOKING_STATUS,
    CREATED_AT
FROM {{ ref('bronze_bookings') }}
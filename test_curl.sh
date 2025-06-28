#!/bin/bash

# DATA EAMPLE
ACCESS="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZWxsZXJJZCI6bnVsbCwic2FsZXNHcm91cHNBbGxvd2VkIjpbIjg1MCIsIjg0MyIsIjg0NCIsIjg0NSIsIjg0NyIsIjgzOCJdLCJ1c2VyX25hbWUiOiJpbnRlZ3JhdGlvbi1ob2xkZWRAZmxhbWVuY29ncmFuYWRhLmNvbSIsInNjb3BlIjpbIjEiXSwiZXhwIjoxNzUxMTExMDgzLCJhdXRob3JpdGllcyI6WyJST0xFX1RISVJEUEFSVFlfTUFTVEVSX0RBVEEiLCJST0xFX1RISVJEUEFSVFlfUFVSQ0hBU0VTIl0sImp0aSI6IjVlZDI4MDFhLWE0NjEtNDdlNi1iNDliLTU2ZWQwMjE2YmM4NiIsImNsaWVudF9pZCI6InRoaXJkLXBhcnR5IiwicG9zQWxsb3dlZCI6WyIyMzcyIl19.UgtcVzYunGOX8AwY3k7_WaWJiO-3SsfVOygCzP92THB7eD56p7w-zEJsWXTlCscA0SD5B6PmnsvNTJdR_m51nwIA4nLba3hUhScgvxEpihs8b_zn1vwlG3egQuMcsYW8e0N8YJtm6uarbujkXlcHCa1209BaEHzAhCJKIyXRNZK923bjJWgsms7R1TAEtOdvsiW-eyfCZcBnVXy6v7vTxpLhYxlDJpuPqPPjIiBbdrA1wSIayp15PDLt8Q0Ib-ZteHOYxEB8paU4qJWo2tiX8LyOU0Ax0-pvHVqjJnvKftbx5UrBhZ97k1zUZOuT9wsmI3ZjgZxwaYOIq6_-YjEB8g"   # tu token
POS=2372                                           # sacado de posAllowed
CID=107                                            # clientId
START=20240625000000                               # 25-06-2024 00:00:00 UTC
END=20240625235959   
LANG="es"                                 # 25-06-2024 23:59:59 UTC

curl -X GET \
  "https://services.clorian.com/ws/purchases?clientId=${CID}&startDatetime=${START}&endDatetime=${END}" \
  -H "Accept: application/json" \
  -H "Authorization: Bearer ${ACCESS}" \
  -H "pos: ${POS}"


# curl -X GET \
#   "https://services.clorian.com/ws/masters/products?clientId=${CID}" \
#   -H "Accept: application/json" \
#   -H "Authorization: Bearer ${ACCESS}" \
#   -H "pos: ${POS}" \
#   -H "Accept-Language: ${LANG}"






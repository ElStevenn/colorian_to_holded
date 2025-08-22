#!/bin/bash

# DATA EAMPLE
ACCESS="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZWxsZXJJZCI6bnVsbCwic2FsZXNHcm91cHNBbGxvd2VkIjpbIjg1MCIsIjg0MyIsIjg0NCIsIjg0NSIsIjg0NyIsIjgzOCJdLCJ1c2VyX25hbWUiOiJpbnRlZ3JhdGlvbi1ob2xkZWRAZmxhbWVuY29ncmFuYWRhLmNvbSIsInNjb3BlIjpbIjEiXSwiZXhwIjoxNzUxMzkxMzc1LCJhdXRob3JpdGllcyI6WyJST0xFX1RISVJEUEFSVFlfTUFTVEVSX0RBVEEiLCJST0xFX1RISVJEUEFSVFlfQklMTFMiLCJST0xFX1RISVJEUEFSVFlfUFVSQ0hBU0VTIl0sImp0aSI6ImI0ZDAwM2E5LTE2ODMtNDVkZS05MWZlLTQ5YTlhMjM1YWIyMyIsImNsaWVudF9pZCI6InRoaXJkLXBhcnR5IiwicG9zQWxsb3dlZCI6WyIyMzcyIl19.LAx-ARt4TepfoX79xbyO-3d206tSU1cS9_7AiaLRMzVZMP8Yvl6x7alvKKZ8cveNq6HEp_mTZq18ZzDiZnVH3nLWwuFrpyEDy3wgV4AIxi-uh-ImmHVoBATEQDbB9umOC4BfEspMyhk5WkeFPfZEMneR7BP8GUwhWvc9CjqwnP6CdBsfyVBD_HrCVyI2_P-awNNsMj587NYcMr3G_ObopllRZ1ZW8UWliBZ510ViC11KwJGaVCIakLJQUHy0N-v5qM39__1JbJ85nKJoB8u51saqFA7w1Bi-BsxbKcDiMTtSiZFGoyerN96t0Ggno6rdihs9Xn44p5OVxlZi1EBgFA"   # tu token
POS=2372          # from posAllowed
CID=107           # clientId
LANG="en"         # or "es"
OUT="bills.json"


STARTTMP=$(date -d "2 years ago" +%s)   # beginning: today minus 2 years
ENDTMP=$(date +%s)                      # ending:   right now

# --- request all invoices in that period -----------------------
curl --request GET \
     --url "https://api.holded.com/api/invoicing/v1/documents/invoice?starttmp=${STARTTMP}&endtmp=${ENDTMP}" \
     --header 'accept: application/json' \
     --header 'key: 0ef9201acaeada511c44dc099d65f070'

# curl -X GET \
#   "https://services.clorian.com/ws/masters/products?clientId=$CID" \
#   -H "Accept: application/json" \
#   -H "Authorization: Bearer $ACCESS" \
#   -H "pos: $POS" \
#   -H "Accept-Language: $LANG" \
#   -o "$OUT"



# mein holded account > 0ef9201acaeada511c44dc099d65f070
# sein holded account > 238c721e02ae11b2850c99b89038cd55 | unique contact: 6870e4f24e485ae888018620
º







# REF_DAY=$(date -u -d "3 days ago" +"%Y%m%d")

# START="${REF_DAY}000000"   # 00:00:00
# END="${REF_DAY}235959"     # 23:59:59

# echo "Fetching ordinary bills for ${REF_DAY} …"

# resp=$(curl -sS \
#   "https://services.clorian.com/ws/bills/normal?clientId=${CID}&startDatetime=${START}&endDatetime=${END}" \
#   -H "Accept: application/json" \
#   -H "Authorization: Bearer ${ACCESS}" \
#   -H "pos: ${POS}")

# echo "$resp" | jq . > "$OUT"
# echo "✓ Bills for ${REF_DAY} saved to ${OUT}"


# echo "All purchases for the last 12 months saved to $OUT"

# curl -X GET \
#   "https://services.clorian.com/ws/masters/products?clientId=${CID}" \
#   -H "Accept: application/json" \
#   -H "Authorization: Bearer ${ACCESS}" \
#   -H "pos: ${POS}" \
#   -H "Accept-Language: ${LANG}"








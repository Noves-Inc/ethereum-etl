set -e

# Run PSQL Server
docker run -p 5432:5432 -d --name postgresql-server -e POSTGRESQL_PASSWORD=postgres bitnami/postgresql:latest

# Connect to SQL Server
psql -d 'postgresql://postgres:postgres@0.0.0.0:5432/ethereum'

ethereumetl export_blocks_and_transactions --start-block 21000000 --end-block 21000100 \
    --blocks-output blocks.csv --transactions-output transactions.csv \
    --provider-uri https://rpc-load-balancer.noves.fi/eth_full/ --batch-size 1


ethereumetl noves_export_blocks_and_transactions --start-block 21000000 --end-block 21000100 \
    --provider-uri https://rpc-load-balancer.noves.fi/eth_full/ --batch-size 1 \
    -d 'postgresql://postgres:postgres@0.0.0.0:5432/ethereum'

ethereumetl extract_csv_column --input transactions.csv --column hash --output transaction_hashes.txt

ethereumetl export_token_transfers --start-block 21000000 --end-block 21000100 \
    --provider-uri https://rpc-load-balancer.noves.fi/eth_full/ --output token_transfers.csv

# Works with Erigon traces
ethereumetl export_traces --start-block 21000000 --end-block 21000100 \
--provider-uri https://rpc-load-balancer.noves.fi/eth_trace-erigon/ --output traces.csv

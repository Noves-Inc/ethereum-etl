set -e

## PSQL Commands

# Run PSQL Server
docker run -p 5432:5432 -d --name postgresql-server -e POSTGRESQL_PASSWORD=postgres bitnami/postgresql:latest

echo "CREATE DATABASE ethereum" | psql -d "postgresql://postgres:postgres@0.0.0.0:5432"

python scripts/create_tables.py "postgresql://postgres:postgres@0.0.0.0:5432/ethereum"

# To connect to the psql database run the following command
# psql -d "postgresql://postgres:postgres@0.0.0.0:5432/ethereum"

export SB=18203830
export EB=18203835
# rm /tmp/receipts.csv /tmp/run.log
echo "DELETE FROM logs; DELETE FROM transactions; DELETE FROM blocks;DELETE FROM contracts;" | psql -d "postgresql://postgres:postgres@0.0.0.0:5432/ethereum"

# Job that streams to the database (blocks and transactions)
# python -m setup.py develop[install]
python -m ethereumetl noves_full_export \
    --start-block $SB --end-block $EB \
    --provider-uri https://rpc-load-balancer.noves.fi/eth_full/ \
    --batch-size 1 \
    -d 'postgresql://postgres:postgres@0.0.0.0:5432/ethereum' &> /tmp/run.log
# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

## Modified to stream results directly to postgres
import time
import click

from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import (
    blocks_and_transactions_item_exporter,
)
from blockchainetl.logging_utils import logging_basic_config
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.utils import check_classic_provider_uri
from ethereumetl.streaming.item_exporter_creator import create_item_exporter
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.exporters.receipts_and_logs_item_exporter import (
    receipts_and_logs_item_exporter,
)
from ethereumetl.jobs.export_contract_job_with_forced_block import ExportContractsWithForcedBlockJob

from sqlalchemy import text
import csv

from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.web3_utils import build_web3

def measure_job_duration(job_name, job_function):
    """Measures and prints the duration of a given job."""
    start_time = time.time()
    print(f"[JOB_DURATION_STATISTICS] Starting job: {job_name}")
    job_function()
    elapsed_time = time.time() - start_time
    print(f"[JOB_DURATION_STATISTICS] Job '{job_name}' completed in {elapsed_time:.2f} seconds.")

def get_contract_addresses_block_number_pairs_from_receipts_csv(filepath):
    with open(filepath) as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]

    return [
        (row.get("contract_address"), row.get("block_number")) for row in rows if row.get("contract_address") != ""
    ]

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-s", "--start-block", default=0, show_default=True, type=int, help="Start block"
)
@click.option("-e", "--end-block", required=True, type=int, help="End block")
@click.option(
    "-b",
    "--batch-size",
    default=100,
    show_default=True,
    type=int,
    help="The number of blocks to export at a time.",
)
@click.option(
    "-p",
    "--provider-uri",
    default="https://mainnet.infura.io",
    show_default=True,
    type=str,
    help="The URI of the web3 provider e.g. "
    "file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io",
)
@click.option(
    "-w",
    "--max-workers",
    default=1,
    show_default=True,
    type=int,
    help="The maximum number of workers.",
)
@click.option(
    "-d",
    "--db-connstring",
    default="postgresql://postgres:postgres@localhost:5432/ethereum",
    help="Database connection string such as: `postgresql://postgres:postgres@localhost:5432/dbname`",
    type=str,
)
@click.option(
    "-c",
    "--chain",
    default="ethereum",
    show_default=True,
    type=str,
    help="The chain network to connect to.",
)
def noves_full_export(
    start_block,
    end_block,
    batch_size,
    provider_uri,
    max_workers,
    db_connstring,
    chain="ethereum",
):
    """Exports blocks and transactions."""
    provider_uri = check_classic_provider_uri(chain, provider_uri)

    receipts_csv_file_path = "/tmp/receipts.csv"

    psql_exporter = create_item_exporter(db_connstring)

    block_and_transactions_job = ExportBlocksJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        max_workers=max_workers,
        item_exporter=psql_exporter,
        export_blocks=True,
        export_transactions=True,
    )

    measure_job_duration("Block and Transactions Export", block_and_transactions_job.run)

    transaction_hashes = psql_exporter.connection.execute(
        text(
            f"SELECT hash FROM transactions WHERE block_number >= {start_block} AND block_number <= {end_block}"
        )
    ).fetchall()

    transaction_hashes = [t[0] for t in transaction_hashes]

    logs_job = ExportReceiptsJob(
        transaction_hashes,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        max_workers=max_workers,
        item_exporter=psql_exporter,
        export_logs=True,
        export_receipts=False,
    )

    measure_job_duration("Logs Export", logs_job.run)

    receipts_job = ExportReceiptsJob(
        transaction_hashes,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        max_workers=max_workers,
        item_exporter=receipts_and_logs_item_exporter(receipts_csv_file_path, None),
        export_logs=False,
        export_receipts=True,
    )

    measure_job_duration("Receipts Export", receipts_job.run)

    contract_addresses_block_number_pairs = get_contract_addresses_block_number_pairs_from_receipts_csv(
        receipts_csv_file_path
    )

    contracts_job = ExportContractsWithForcedBlockJob(
        contract_addresses_block_number_pairs_iterable=contract_addresses_block_number_pairs,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=psql_exporter,
        max_workers=max_workers,
    )
    measure_job_duration("Contracts Export", contracts_job.run)

    token_transfers_job = ExportTokenTransfersJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        web3=ThreadLocalProxy(lambda: build_web3(get_provider_from_uri(provider_uri))),
        item_exporter=psql_exporter,
        max_workers=max_workers,
        tokens=None)
    
    measure_job_duration("Token Transfers Export", token_transfers_job.run)
#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#     "asyncio",
#     "uvloop",
#     "httpx",
#     "polars"
# ]
# ///
from pathlib import Path
import re

import asyncio
import httpx
import polars as pl


JPAGE_ROOT = "https://www.tekjobs.net"
COMPANY_PAGE_URL = f"{JPAGE_ROOT}/staticpage/companyPageDetails/651c4a142544e25a8b6115ed"
async def rip_jroot_page(client: httpx.AsyncClient) -> pl.DataFrame:
    res = await client.get(COMPANY_PAGE_URL)

    empid = []
    results = {}
    fs = [
        'id',
        'name',
        'title',
        'vtype',
        'empexp',
        'empcity',
        'empstate',
        'empreloc',
        'resume',
    ]
    for rowm in re.finditer(r'<tr.*?>(.*?)<\/tr>', res.content.decode(), re.S):
        row = rowm.group(1)
        tds = re.finditer(r'<td.*?>(.*?)<\/td>', row, re.S)
        try:
            # only if we are in the tbody is the first td an integer
            empid = int(next(tds).group(1))
        except Exception:
            continue
        namefield = next(tds).group(1)
        l, r = namefield.split('>', 1)
        empname, _ = r.split('<', 1)
        empresumeslug = l.rsplit("/", 1)[1].split("\"", 1)[0]
        emptitle = next(tds).group(1)
        empvtype = next(tds).group(1)
        empexp = int(next(tds).group(1))
        empcity = next(tds).group(1)
        empstate = next(tds).group(1)
        empreloc = next(tds).group(1) == 'Yes'

        for f, v in zip(fs, [
            empid,
            empname,
            emptitle,
            empvtype,
            empexp,
            empcity,
            empstate,
            empreloc,
            empresumeslug,
        ]):
            results.setdefault(f, []).append(v)

    return pl.DataFrame(results)


RESUME_URL = lambda rslug: f"{JPAGE_ROOT}/employer/searchResume/resume/{rslug}"
async def fetch_resume(client: httpx.AsyncClient, slug: str) -> bytes | None:
    rurl = RESUME_URL(slug)
    print(f'[{slug}]: worker')
    res = await client.get(rurl)
    html = res.content.decode()
    # remove commented out iframes with stale signed uris
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)

    url = None
    for m in re.finditer(r'https://tekjobs-resumes\.s3\.amazonaws\.com/[^\s"\'<>]+', html, re.S):
        url = m.group(0)
        print(f'[{slug}]: trying {url}:')
        res = await client.get(url)
        if res.status_code == 200:
            return res.content

    print(f'[{slug}]: no valid url candidate found')



async def fetch_resumes(
    client: httpx.AsyncClient,
    employees: pl.DataFrame,
    checkpoint: pl.DataFrame | None = None,
    max_concurrent_workers: int = 5,
) -> pl.DataFrame:
    sem = asyncio.Semaphore(max_concurrent_workers)

    async def worker(slug) -> bytes | None:
        try:
            async with sem:
                return await fetch_resume(client, slug)
        except Exception:
            import traceback
            print(traceback.format_exc())

    to_run = employees
    if checkpoint is not None:
        to_run = (
            employees
                .join(checkpoint, 'resume', how='left')
                .filter(pl.col.blob.is_null())
        )

    slugs = []
    ts = []
    async with asyncio.TaskGroup() as tg:
        for slug, in to_run.select('resume').iter_rows():
            slugs.append(slug)
            ts.append(tg.create_task(worker(slug)))

    outp = pl.DataFrame({
        'resume': slugs,
        'blob': await asyncio.gather(*ts),
    })

    if checkpoint is not None:
        outp = pl.concat([checkpoint, outp])

    return outp


LOCAL_DB_PATH = Path('store')
async def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="CLI with run and unpack subcommands."
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run the main process."
    )

    run_parser.add_argument(
        "-c", "--checkpoint",
        choices=["restart", "use"],
        required=True,
        help="Checkpoint behavior: restart (rebuild resumes from source), or use (only attempt to get resumes not current checkpointed)."
    )

    up_parser = subparsers.add_parser(
        "unpack",
        help="Unpack resources."
    )

    up_parser.add_argument(
        "-o", "--output-dir",
        required=True,
        help="where to unpack the resumes to",
    )

    args = parser.parse_args()

    employees_path = (LOCAL_DB_PATH / 'employees.parquet')
    checkpoint_path = (LOCAL_DB_PATH / 'resumes.parquet')
    store_path = (LOCAL_DB_PATH / 'resumes')
    match args.command:
        case 'run':

            async with httpx.AsyncClient() as client:
                if employees_path.exists():
                    emp_data = pl.read_parquet(employees_path)
                else:
                    print('getting employee data')
                    emp_data = await rip_jroot_page(client)
                    emp_data.write_parquet(employees_path)


            checkpoint = None
            if checkpoint_path.exists() and args.checkpoint == 'use':
                checkpoint = pl.read_parquet(employees_path)

            async with httpx.AsyncClient() as client:
                resume_data = await fetch_resumes(client, emp_data, checkpoint)
                resume_data.write_parquet(checkpoint_path)

            output = (
                emp_data.join(resume_data, 'resume').with_columns(
                    file_type=pl.when(pl.col.blob.bin.starts_with(b'%PDF'))
                        .then(pl.lit('.pdf'))
                        .when(pl.col.blob.bin.starts_with(b'PK'))
                        .then(pl.lit('.docx'))
                )
            )

            output.write_parquet(store_path, partition_by=['vtype', 'file_type'])

        case 'unpack':
            assert employees_path.exists() and checkpoint_path.exists() and store_path.exists(), """
run the 'run' subcommand first!
"""
            data = pl.read_parquet(store_path, hive_partitioning=True)


            with_resumes = data.filter(pl.col.file_type.is_not_null())
            print(f'{len(with_resumes)} / {len(data)} records with resumes, unpacking')

            for n, r, blob, ft in with_resumes.select('name', 'resume', 'blob', 'file_type').iter_rows():
                op = Path(args.output_dir)
                op.mkdir(exist_ok=True)
                with open(f'{args.output_dir}/{n.replace(' ', '_').lower()}_{r}{ft}', 'wb') as f:
                    f.write(blob)


if __name__ == "__main__":
    import uvloop as io
    io.run(main())

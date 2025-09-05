
import pyarrow.dataset as ds
import pyarrow.compute as pc
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Union
Row = Union[dict, Tuple]

def stream_parquet_rows(
    path: str,
    columns: Optional[List[str]] = None,
    rename: Optional[Dict[str, str]] = None,
    *,
    batch_size: int = 100_000,
    filter_expr: Optional[pc.Expression] = None,
    as_dict: bool = True,
    use_threads: bool = True,
) -> Iterator[Row]:
    dataset = ds.dataset(path, format="parquet")
    # print(f"dataset: {dataset}")
    schema_cols = [f.name for f in dataset.schema]

    # Normalize projection list (what to load)
    proj = columns if columns is not None else schema_cols
    proj_existing = [c for c in proj if c in schema_cols]
    proj_missing  = [c for c in proj if c not in schema_cols]
    # print(f"Projection: {proj_existing} | Missing: {proj_missing}")
    # Output names (after rename)
    rename = rename or {}
    out_order = [rename.get(c, c) for c in proj]

    # Create scanner
    scanner = dataset.scanner(
        columns=proj_existing,
        filter=filter_expr,
        batch_size=batch_size,
        use_threads=use_threads
    )

    # Stream batches
    for batch in scanner.to_batches():
        # Map column name -> Arrow array
        colmap = {name: batch.column(i) for i, name in enumerate(proj_existing)}
        n = batch.num_rows

        # Emit rows
        for i in range(n):
            if as_dict:
                rec = {}
                for src in proj:
                    out_name = rename.get(src, src)
                    if src in colmap:
                        rec[out_name] = colmap[src][i].as_py()
                    else:
                        rec[out_name] = None  # keep stable shape if column absent
                yield rec
            else:
                tup = []
                for src in proj:
                    tup.append(colmap[src][i].as_py() if src in colmap else None)
                yield tuple(tup)

import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd

dataset = ds.dataset("opdi_clean.parquet", format="parquet")

total_rows = dataset.count_rows()
print("Total rows:", total_rows)

for col in dataset.schema.names:
    nulls = 0
    for batch in dataset.to_batches(columns=[col]):
        nulls += pc.sum(pc.is_null(batch.column(0))).as_py()
    print(f"{col}: {nulls} nulls ({nulls/total_rows*100:.2f}%)")

dsum = ds.dataset("opdi_clean.parquet", format="parquet")
first_batch = next(dsum.to_batches(batch_size=5))
df_head = first_batch.to_pandas()
with pd.option_context(
    "display.max_columns", None,
    "display.width", None,
    "display.max_colwidth", None
):
    print(df_head)

from dagster import In, Out, job
from dagster.utils import script_relative_path

import dagstermill as dm

load_data = dm.define_dagstermill_op(
    "load_data",
    notebook_path=script_relative_path("load_data.ipynb"),
    outs={"numbers": Out(list, description="list of numbers")}
)

read_data = dm.define_dagstermill_op(
    "read_data",
    notebook_path=script_relative_path("read_data.ipynb"),
    ins={"numbers": In(list, description="list of numbers obtained from load_data.ipynb")}
)


@job(
    resource_defs={
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    }
)
def main_job():
    numbers = load_data()
    read_data(numbers)

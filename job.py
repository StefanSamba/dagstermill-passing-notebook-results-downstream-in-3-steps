import dagstermill as dm
from dagster import In, Out, job
from dagster.utils import script_relative_path

load_data = dm.define_dagstermill_op(
    # Name of the op:
    "load_data",
    # Path to the notebook
    notebook_path=script_relative_path("load_data.ipynb"),
    # The op's outputs.
    outs={"numbers": Out(list, description="list of numbers")}
)


read_data = dm.define_dagstermill_op(
    # Name of the op:
    "read_data",
    # Path to the notebook
    notebook_path=script_relative_path("read_data.ipynb"),
    # The op's inputs.
    ins={"numbers": In(list, description="list of numbers obtained from\
                                          load_data.ipynb")}
)


@job(
    resource_defs={
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    }
)
def main_job():
    numbers = load_data()
    read_data(numbers)

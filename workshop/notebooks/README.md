# Steps to try out mlmd_query notebook.

- [Install](https://jupyter.org/install) jupyter notebook on your desktop.
- Navigate to this directory in your CitC client.

  ```shell
  g4d <your client>
  cd experimental/users/vemmadi/tfx_demo/
  jupyter notebook
  ```
- Navigate to http://localhost:8888 in your browser.
- Generate the test data

  - Open the `mlmd_test_data.ipynb` notebook.
  - Follow the instructions to run TFX OSS as described in cell #3.
  - Run all cells.
- Run the query notebook
  - Open the `mlmd_query.ipynb` notebook.
  - Run all cells.

    NOTE: The cells that invoke tensorboard will not run to completion. To
    continue running the cells below the tensorboard cell, you must interrupt
    the Jupyter kernel.

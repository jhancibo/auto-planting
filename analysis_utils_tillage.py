# %%
import os
from collections import defaultdict
import json
import logging
import numpy as np
import pandas as pd
import re
from pathlib import Path, PosixPath
from typing import Dict, List, Optional, Tuple
from pandas.core.frame import DataFrame
import pint
import pint_pandas
import inspect

ureg = pint_pandas.PintType.ureg


# %%
def set_up_logging(salus_version: str):
    logging.basicConfig(level=logging.INFO, filename=f"Carbon_Analysis_{salus_version}.log",
                        filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logging.info("Date {}".format(pd.to_datetime("today")))
    logging.info("Working directory {}".format(os.path.join(os.getcwd())))
    return 0

# %%
def get_salus_result_paths(data_path: Path, salus_version: str) -> List[Path]:
    """
    Scans the path, could be an s3 path (using S3Path type),
    for all json files satisfying the glob
        */salus-*/SalusOutputs/FarmerFull/*.json

    Returns as list of paths of salus results.
    (The paths also include information about salus version
    and duplicate information about the paper source and
    experiment id.)
    """
    # loop over all directories, and all salus versions
    output_file_paths = []
    for output_file in data_path.glob(f"*/{salus_version}/SalusOutputs/FarmerFull/*.json"):
        output_file_paths.append(output_file)

    return output_file_paths
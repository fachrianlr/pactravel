import subprocess as sp
import luigi
import datetime
import time
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from config.logging_conf import logger
from config.sentry_conf import SENTRY_LOGGING

load_dotenv()


class GlobalParams(luigi.Config):
    current_date = luigi.Parameter(default = datetime.datetime.now().strftime('%Y-%m-%d'))
    dbt_folder = luigi.Parameter(default = "dbt")


class SnapshotData(luigi.Task):

    get_current_date = GlobalParams().current_date
    
    def output(self):
        return luigi.LocalTarget(f"logs/snapshot_data_{self.get_current_date}.txt")

    def run(self):
        logger.info("Start Snapshot Data Process")

        try:
            # Start the subprocess
            with sp.Popen(
                "dbt debug && dbt deps && dbt snapshot",
                stdout=sp.PIPE,
                stderr=sp.PIPE,
                text=True,
                shell=True,
                cwd=GlobalParams().dbt_folder
            ) as p1:
                
                for line in p1.stdout:
                    logger.info(line.strip())

                for line in p1.stderr:
                    logger.error(line.strip())

                p1.wait()
            
            with open(f"logs/snapshot_data_{self.get_current_date}.txt", "w") as file:
                file.write("Snapshot Data Success")

            logger.info("Snapshot Data Process Success")

        except Exception as e:
            SENTRY_LOGGING.capture_exception(e)
            logger.error("Failed Process", e)


if __name__ == "__main__":
    luigi.build(
        [SnapshotData()], 
        local_scheduler=True
    )

import subprocess as sp
import luigi
import datetime
import time
import logging
import psycopg2
import pandas as pd
import os
from sqlalchemy import create_engine


class GlobalParams(luigi.Config):
    current_date = luigi.Parameter(default = datetime.datetime.now().strftime('%Y-%m-%d'))
    dbt_folder = luigi.Parameter(default = "dbt")


class dbtDebug(luigi.Task):

    get_current_date = GlobalParams().current_date

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"logs/dbt_debug_logs_{self.get_current_date}.log")
    
    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt debug",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            cwd = GlobalParams().dbt_folder,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success Run dbt debug process")

                else:
                    logging.error("Failed to run dbt debug")

            time.sleep(2)
        
        except Exception:
            logging.error("Failed Process")

class dbtDeps(luigi.Task):

    get_current_date = GlobalParams().current_date

    def requires(self):
        return dbtDebug()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_deps_logs_{self.get_current_date}.log")

    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt deps",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            cwd = GlobalParams().dbt_folder,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success installing dependencies")

                else:
                    logging.error("Failed installing dependencies")

            time.sleep(2)

        except Exception:
            logging.error("Failed Process")

class dbtRun(luigi.Task):

    get_current_date = GlobalParams().current_date

    def requires(self):
        return dbtDeps()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_run_logs_{self.get_current_date}.log")
    
    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt run",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            cwd = GlobalParams().dbt_folder,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success running dbt data model")

                else:
                    logging.error("Failed running dbt model")

            time.sleep(2)

        except Exception:
            logging.error("Failed Process")

class dbtTest(luigi.Task):

    get_current_date = GlobalParams().current_date

    def requires(self):
        return dbtRun()
    
    def output(self):
        return luigi.LocalTarget(f"logs/dbt_test_logs_{self.get_current_date}.log")

    def run(self):
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt test",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            cwd = GlobalParams().dbt_folder,
                            check = True)
                
                if p1.returncode == 0:
                    logging.info("Success running dbt test")

                else:
                    logging.error("Failed running testing")

            time.sleep(2)

        except Exception:
            logging.error("Failed Process")


class ExtractData(luigi.Task):

    get_current_date = GlobalParams().current_date

    db_name = 'pactravel'
    db_host = 'localhost'
    db_user = 'postgres'
    db_password = 'mypassword'
    db_port = 5437

    def output(self):
        output_data = os.path.join('raw_data', str(self.get_current_date))
        return luigi.LocalTarget(output_data)

    def run(self):
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )

        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()

        all_data = []
        for table in tables:
            table_name = table[0]
            print(f"Extracting data from table: {table_name}")
            cursor.execute(f"SELECT * FROM {table_name};")

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            
            output_file = os.path.join('raw_data', str(self.get_current_date), f"{table_name}.csv")
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                df.to_csv(f, index=False, encoding='utf-8')

        cursor.close()
        conn.close()


class LoadData(luigi.Task):
    get_current_date = GlobalParams().current_date
    
    db_name = 'pactravel-dwh'
    db_host = 'localhost'
    db_user = 'postgres'
    db_password = 'mypassword'
    db_port = 5438
    db_schema = 'pactravel'
    
    def requires(self):
        return ExtractData()

    def output(self):
        return luigi.LocalTarget(f"logs/load_data_logs_{self.get_current_date}.log")

    
    def run(self):
        output_status = "Success"
        raw_data = os.path.join('raw_data', str(self.get_current_date))
        try:
            for file_name in os.listdir(raw_data):
                file_path = os.path.join(raw_data, file_name)
                
                df = pd.read_csv(file_path)
                table_name = f"{self.db_schema}.{os.path.splitext(file_name)[0]}"
                
                engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
                df.to_sql(table_name, engine, if_exists='replace', index=False)

            logging.info("Start Load Data Process")
        except Exception as e:
            output_status = "Error"
            logging.error("Failed Process", e)

        with open(f"logs/load_data_{self.get_current_date}.txt", 'w') as file:
            file.write(output_status)

if __name__ == "__main__":
    luigi.build(
        [LoadData()], 
        local_scheduler=True
    )

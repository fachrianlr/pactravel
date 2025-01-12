import subprocess as sp
import luigi
import datetime
import time
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from config.logging_conf import logger

load_dotenv()


class GlobalParams(luigi.Config):
    current_date = luigi.Parameter(default = datetime.datetime.now().strftime('%Y-%m-%d'))
    dbt_folder = luigi.Parameter(default = "dbt")


class ExtractData(luigi.Task):

    get_current_date = GlobalParams().current_date

    db_name = os.getenv('SRC_POSTGRES_DB')
    db_host = os.getenv('SRC_POSTGRES_HOST')
    db_user = os.getenv('SRC_POSTGRES_USER')
    db_password = os.getenv('SRC_POSTGRES_PASSWORD')
    db_port = os.getenv('SRC_POSTGRES_PORT')

    def output(self):
        output_data = os.path.join('raw_data', str(self.get_current_date))
        return luigi.LocalTarget(output_data)

    def run(self):
        logger.info("Start Extract Data Process")
        
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                logger.info(f"Extracting data from table: {table_name}")
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
            
        except Exception as e:
            logger.error("Failed Process", e)


class LoadData(luigi.Task):
    get_current_date = GlobalParams().current_date
    
    db_name = os.getenv('DWH_POSTGRES_DB')
    db_host = os.getenv('DWH_POSTGRES_HOST')
    db_user = os.getenv('DWH_POSTGRES_USER')
    db_password = os.getenv('DWH_POSTGRES_PASSWORD')
    db_port = os.getenv('DWH_POSTGRES_PORT')
    db_schema = os.getenv('DWH_POSTGRES_SCHEMA')

    def requires(self):
        return ExtractData()

    def output(self):
        return luigi.LocalTarget(f"logs/load_data_{self.get_current_date}.log")

    
    def run(self):
        logger.info("Start Load Data Process")
        raw_data = os.path.join('raw_data', str(self.get_current_date))
        try:
            for file_name in os.listdir(raw_data):
                file_path = os.path.join(raw_data, file_name)
                
                df = pd.read_csv(file_path)
                table_name = os.path.splitext(file_name)[0]
                logger.info("table_name: ", table_name)
                
                
                engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
                df.to_sql(table_name, engine, schema=self.db_schema, if_exists='replace', index=False)
        except Exception as e:
            logger.error("Failed Process Load Data", e)


class TransformData(luigi.Task):

    get_current_date = GlobalParams().current_date

    def requires(self):
        return LoadData()
    
    def output(self):
        return luigi.LocalTarget(f"logs/transform_data_{self.get_current_date}.log")

    def run(self):
        logger.info("Start Transform Data Process")

        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run("dbt debug && dbt deps && dbt run --models transform && dbt test",
                            stdout = f,
                            stderr = sp.PIPE,
                            text = True,
                            shell = True,
                            cwd = GlobalParams().dbt_folder,
                            check = True)
                
                if p1.returncode == 0:
                    logger.info("Success running dbt data model")

                else:
                    logger.error("Failed running dbt model")

            time.sleep(2)

        except Exception as e:
            logger.error("Failed Process", e)
            

if __name__ == "__main__":
    luigi.build(
        [TransformData()], 
        local_scheduler=True
    )

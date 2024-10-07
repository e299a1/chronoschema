import os
import re
import shutil
import pyodbc
import urllib3
import sqlalchemy as sql
import mssqlscripter.main as scripter
from glob import glob
from datetime import datetime

import unicodedata

def slugify(value:str, allow_unicode:bool=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')

class migration():
    def __init__ (self, name:str, base_dir:str):
        self.name:str = name
        self.base_dir:str = base_dir
        self.staging_dir:str = fr"{self.base_dir}\.stg"
        self.current_migration:str = slugify(fr"{datetime.now().strftime("%Y%m%d%H%M%S")}-{name}")
        self.sql_conn_str:str     
        self.sql_conn_url:sql.URL 
        self.sql_engine:sql.Engine

    def from_db(self, sources: list[str], generate_creation_migrations:bool=False, overwrite:bool = False):
        """
        Generates the database schema for each of the "[server].[database]" items passed to the function.
        Optionally also generates the inital database creation script in the migrations folder.
        """
        #TODO: Add a creation order marker to filenames, as it's relevant to the execution order when spawning DBs from the scripted schemas.

        for source in sources:
            source_server, source_db    = source.strip("[").strip("]").split("].[")
            self.sql_conn_str           = fr"Driver={{{[x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]}}}; Server={source_server};Database=master;Trusted_Connection=yes;"
            self.sql_conn_url           = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": self.sql_conn_str})        
            self.sql_engine             = sql.create_engine(self.sql_conn_url, connect_args = {"autocommit":True})
            self.current_migration      = slugify(fr"{datetime.now().strftime("%Y%m%d%H%M%S")}-{source_db} creation script")
            db_stg_dir                  = f"{self.staging_dir}\\{self.current_migration}"


            if os.path.isdir(db_stg_dir):
                shutil.rmtree(db_stg_dir)

            if not os.path.isdir(f"{db_stg_dir}\\migrations"):
                os.makedirs(f"{db_stg_dir}\\migrations")

            if generate_creation_migrations:
                print(fr"Scripting initial schema creation for {source}...")
                scripter.main([
                    "--connection-string", fr"Server={source_server};Database={source_db};Trusted_Connection=yes;",
                    "-f", f"{db_stg_dir}\\migrations\\{self.current_migration}.sql",
                    "--script-create",
                    #"--change-tracking",
                    "--exclude-headers",
                    "--exclude-defaults",
                    #"--display-progress",
                ])


            print(fr"Scripting schema layout for {source}...")
            scripter.main([
                "--connection-string", fr"Server={source_server};Database={source_db};Trusted_Connection=yes;",
                "-f", f"{db_stg_dir}\\schema\\{source_server}\\{source_db}",
                "--file-per-object",
                "--script-create",
                #"--change-tracking",
                "--exclude-headers",
                "--exclude-defaults",
                #"--display-progress",
            ])

                
            schema_base_dir = f"{self.base_dir}\\schema\\{source_server}\\{source_db}"
            if overwrite:
                print(fr"Overwriting existing files at {schema_base_dir}...")
                if os.path.isdir(schema_base_dir):
                    for root, _, files in os.walk(schema_base_dir):
                        for file in files:
                            if file.endswith(".sql"):
                                #print(fr"Deleting {file}...")
                                os.remove(os.path.join(root, file))

            print(fr"Moving files out of \.stg...")
            for root, _, files in os.walk(db_stg_dir):
                for file in files:
                    target_dir = root.replace(db_stg_dir,"")
                    full_target_dir = fr"{self.base_dir}\{target_dir}"
                    #print(fr"Moving {target_dir}\{file}...")
                    if not os.path.isdir(full_target_dir):
                        #print(fr"Creating {target_dir} folder...")
                        os.makedirs(full_target_dir)
                    shutil.move(os.path.join(root, file), os.path.join(full_target_dir, file))
        
            if os.path.isdir(db_stg_dir):
                shutil.rmtree(db_stg_dir)

    def migration_to_db(self, target_server:str, target_migrations: list[str]):
        """
        Runs a list of migrations against the chosen server."
        """
        #TODO: Allow for inferred selection (i.e.:"run this migration and all that came after it")

        for target in target_migrations:
            self.sql_conn_str           = fr"Driver={{{[x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]}}}; Server={target_server};Database=master;Trusted_Connection=yes;"
            self.sql_conn_url           = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": self.sql_conn_str})        
            self.sql_engine             = sql.create_engine(self.sql_conn_url, connect_args = {"autocommit":True})

            print(fr"Trying to connect to {target_server}...")

            with open(f"{self.base_dir}\\migrations\\{target}.sql", "r", encoding="utf-8") as f:
                batches = re.split(r"(?<=)GO\n", f.read())[:-1]
            
            with self.sql_engine.connect() as connection:
                print(fr"Executing {len(batches)} batches...")
                for i, batch in enumerate(batches):
                    try:
                        _ = connection.execute(sql.text(batch))
                        #print(fr"GO - - - > Batch {i+1}/{len(batches)} OK!")
                    except Exception as exc:
                        print(fr"Failed on batch {i+1}/{len(batches)}!")
                        print(exc)


    def schema_to_db(self, target_addresses: list[str], overwrite:bool = False):
        """
        Runs a list of schema creations scripts against the chosen server."
        """
        #TODO: Follow proper creation order based on dependencies.

        for target in target_addresses:
            target_server, target_db    = target.strip("[").strip("]").split("].[")
            self.sql_conn_str           = fr"Driver={{{[x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]}}}; Server={target_server};Database=master;Trusted_Connection=yes;"
            self.sql_conn_url           = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": self.sql_conn_str})        
            self.sql_engine             = sql.create_engine(self.sql_conn_url, connect_args = {"autocommit":True})

            print(fr"Trying to connect to {target_server}...")

            for root, _, files in os.walk(f"{self.base_dir}\\schema\\{target_server}\\{self.current_migration}"):
                for file in files:
                    with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                        batches = re.split(r"(?<=)GO\n", f.read())[:-1]
                    
                    with self.sql_engine.connect() as connection:
                        print(fr"Preparing to execute migration scripts...")
                        if overwrite:
                            print(fr"Dropping [{target_db}] if it already exists...")
                            _ = connection.execute(sql.text(fr"DROP DATABASE IF EXISTS [{target_db}];"))

                        print(fr"Executing {len(batches)} batches...")
                        for i, batch in enumerate(batches):
                            try:
                                _ = connection.execute(sql.text(batch))
                                #print(fr"GO - - - > Batch {i+1}/{len(batches)} OK!")
                            except Exception as exc:
                                print(fr"Failed on batch {i+1}/{len(batches)}!")
                                print(exc)


    def new_blank(self):
        """
        Generates a blank .sql migration script following the proper filename formating.
        """
        if not os.path.isdir(fr"{self.base_dir}\\migrations"):
            os.makedirs(fr"{self.base_dir}\\migrations")
        with open(fr"{self.base_dir}\\migrations\\{self.current_migration}.sql", "w") as file:
            _ = file.write(fr"-- {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - {self.name}" )
        

    def cleanup(self, target_files:str, name_swaps:dict[str, str], regex_remove:str, swap_filenames:bool=True, remove_empty_dirs:bool=True):
        """
        General cleanup utility function.
        Makes sure the entire folder (schemas and migrations) follow the desired encoding, object names, and no empty unused directories.
        Also allows for easy object renaming/remapping.
        """
        print(fr"Cleaning up {target_files}...")
        for file in glob(target_files, recursive=True):
            #print(fr"Cleaning up {file}...")
            newfile = file
            with open(file, "r", encoding="utf-8-sig") as f:
                text = f.read()
            text = re.sub(regex_remove, "", text, flags=re.MULTILINE)
            for source, target in name_swaps.items():
                text = text.replace(source, target)
                if swap_filenames:
                    newfile = newfile.replace(source, target)

            with open(file, "w", encoding="utf-8") as f:
                _ = f.write(text)

            if swap_filenames and file != newfile:
                newfile_dir = newfile.rsplit('\\', 1)[0]
                if not os.path.isdir(newfile_dir):
                    os.makedirs(newfile_dir)
                os.rename(file, newfile)

        if remove_empty_dirs:
            deleted:set[str] = set()
            for current_dir, subdirs, files in os.walk(self.base_dir, topdown=False):
                still_has_subdirs = False
                for subdir in subdirs:
                    if os.path.join(current_dir, subdir) not in deleted:
                        still_has_subdirs = True
                        break
                if not any(files) and not still_has_subdirs:
                    os.rmdir(current_dir)
                    deleted.add(current_dir)




if __name__.endswith("__main__"):
    urllib3.disable_warnings()
    #TODO: Get the CLI working

    base_dir = r"R:\DADOS E BI\Dados\Testes\databases" 


    mig = migration(r"chore: sync repo do cliente com repo interno", base_dir)
    #mig.new_blank()
    mig.from_db([
                "[003sql001prd].[db_app]",
                "[003sql001prd].[db_prod_d0]",
                "[003sql001prd].[db_prod_d-1]", 
                "[003sql001prd].[db_usuarios]", 
                "[003sql001prd].[db_Serasa_producao]",
                ],
                generate_creation_migrations=False,             
                overwrite=True)
    mig.cleanup(target_files=mig.base_dir+"\\schema\\**\\*.sql",
                regex_remove=r"^(\( NAME = N'| ON  | LOG ON).*",
                name_swaps  ={
                             "003sql001prd"      : "003sql001dev",
                             "db_app"            : "db_app_dev",
                             "db_prod_d0"        : "db_dev_d0",
                             "db_prod_d-1"       : "db_dev_d-1",
                             "db_usuarios"       : "db_usuarios_dev",
                             "db_Serasa_producao": "db_Serasa_dev",
                             "db_producao"       : "db_dev",
                             })
    mig.schema_to_db([
                    "[003sql001dev].[db_app_dev]",
                    "[003sql001dev].[db_dev_d0]",
                    "[003sql001dev].[db_dev_d-1]", 
                    "[003sql001dev].[db_usuarios]", 
                    "[003sql001dev].[db_Serasa_dev]",
                    ],
                    overwrite=False)
    mig.migration_to_db("[003sql001dev]",
                        [
                        "20241001123018-feat-espelhar-views-do-banco-legado-no-banco-de-usuarios", 
                        "20241002120041-fix-corrigir-problema-de-permissionamento-do-setor-de-planejamento-nas-atualizacoes-do-mop",
                        ])





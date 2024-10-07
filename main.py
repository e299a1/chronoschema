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


def from_db(sources: list[str], generate_creation_migrations:bool=False, overwrite:bool=False, base_dir:str=""):
    """
    Generates the database schema for each of the "[server].[database]" items passed to the function.
    Optionally also generates the inital database creation script in the migrations folder.
    """
    #TODO: Add a file creation order marker to filenames, as it's relevant to the execution order when spawning DBs from the scripted schemas.
    #TODO: Make it so the creation order marks are base os file contents and proper dependency tracking.
    
    if not base_dir:
        base_dir = os.getcwd()

    for source in sources:
        source_server, source_db    = source.strip("[").strip("]").split("].[")
        db_stg_dir             = f"{base_dir}\\.stg\\{slugify(source)}"


        if os.path.isdir(db_stg_dir):
            shutil.rmtree(db_stg_dir)

        if not os.path.isdir(f"{db_stg_dir}\\migrations"):
            os.makedirs(f"{db_stg_dir}\\migrations")

        if generate_creation_migrations:
            current_migration      = slugify(fr"{datetime.now().strftime("%Y%m%d%H%M%S")}-{source_db} creation script")
            print(fr"Scripting initial schema creation for {source}...")
            scripter.main([
                "--connection-string", fr"Server={source_server};Database={source_db};Trusted_Connection=yes;",
                "-f", f"{db_stg_dir}\\migrations\\{current_migration}.sql",
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

            
        schema_base_dir = f"{base_dir}\\schema\\{source_server}\\{source_db}"
        if overwrite:
            print(fr"Overwriting existing files at {schema_base_dir}...")
            if os.path.isdir(schema_base_dir):
                for root, _, files in os.walk(schema_base_dir):
                    for file in files:
                        if file.endswith(".sql"):
                            os.remove(os.path.join(root, file))

        print(fr"Moving files out of \.stg...")
        for root, _, files in os.walk(db_stg_dir):
            for file in files:
                target_dir = root.replace(db_stg_dir,"")
                full_target_dir = fr"{base_dir}\{target_dir}"
                if not os.path.isdir(full_target_dir):
                    os.makedirs(full_target_dir)
                shutil.move(os.path.join(root, file), os.path.join(full_target_dir, file))
    
        if os.path.isdir(db_stg_dir):
            shutil.rmtree(db_stg_dir)


def migration_to_db(target_server:str, target_migrations: list[str], base_dir:str=""):
    """
    Runs a list of migrations against the chosen server."
    """
    #TODO: Allow for inferred selection (i.e.:"run this migration and all that came after it", "run last 3 migrations")
    
    if not base_dir:
        base_dir = os.getcwd()

    for target in target_migrations:
        sql_conn_str           = fr"Driver={{{[x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]}}}; Server={target_server};Database=master;Trusted_Connection=yes;"
        sql_conn_url           = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": sql_conn_str})        
        sql_engine             = sql.create_engine(sql_conn_url, connect_args = {"autocommit":True})

        print(fr"Trying to connect to {target_server}...")

        with open(f"{base_dir}\\migrations\\{target}.sql", "r", encoding="utf-8") as f:
            batches = re.split(r"(?<=)GO\n", f.read())[:-1]

        with sql_engine.connect() as connection:
            print(fr"Executing {len(batches)} batches...")
            for i, batch in enumerate(batches):
                try:
                    _ = connection.execute(sql.text(batch))
                    #print(fr"GO - - - > Batch {i+1}/{len(batches)} OK!")
                except Exception as exc:
                    print(fr"Failed on batch {i+1}/{len(batches)}!")
                    print(exc)


def schema_to_db(target_addresses: list[str], overwrite:bool = False, base_dir:str=""):
    """
    Runs a list of schema creations scripts against the chosen server."
    """
    #TODO: Follow proper creation order based on dependencies.
    
    if not base_dir:
        base_dir = os.getcwd()

    for target in target_addresses:
        target_server, target_db    = target.strip("[").strip("]").split("].[")
        sql_conn_str           = fr"Driver={{{[x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]}}}; Server={target_server};Database=master;Trusted_Connection=yes;"
        sql_conn_url           = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": sql_conn_str})        
        sql_engine             = sql.create_engine(sql_conn_url, connect_args = {"autocommit":True})

        print(fr"Trying to connect to {target_server}...")

        for root, _, files in os.walk(f"{base_dir}\\schema\\{target_server}\\{target_db}"):
            for file in files:
                with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                    batches = re.split(r"(?<=)GO\n", f.read())[:-1]
                
                with sql_engine.connect() as connection:
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


def new_blank(name:str, base_dir:str=""):
    """
    Generates a blank .sql migration script following the proper filename formating.
    """

    if not base_dir:
        base_dir = os.getcwd()

    if not os.path.isdir(fr"{base_dir}\\migrations"):
        os.makedirs(fr"{base_dir}\\migrations")
    with open(fr"{base_dir}\\migrations\\{slugify(fr"{datetime.now().strftime("%Y%m%d%H%M%S")}-{name}")}.sql", "w") as file:
        _ = file.write(fr"-- {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - {name}" )
    

def cleanup(target_files:str, name_swaps:dict[str, str], regex_remove:str, swap_filenames:bool=True, remove_empty_dirs:bool=True, base_dir:str=""):
    """
    General cleanup utility function.
    Makes sure the entire folder (schemas and migrations) follow the desired encoding, object names, and no empty unused directories.
    Also allows for easy object renaming/remapping.
    """

    if not base_dir:
        base_dir = os.getcwd()

    if not os.path.isabs(target_files):
        target_files = os.path.join(base_dir, target_files)

    files = glob(target_files, recursive=True)
    print(fr"Cleaning up {len(files)} files matching to {target_files}...")
    for i, file in enumerate(files):
        newfile = file
        try:
            with open(file, "r", encoding="utf-8-sig") as f:
                text = f.read()
        except Exception as exc:
            print(fr"Got error while cleaning up file {i}/{len(files)} at {file}...")
            print(exc)
            return

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
        for current_dir, subdirs, files in os.walk(base_dir, topdown=False):
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
    

    new_blank(r"chore: sync repo do cliente com repo interno", r"R:\DADOS E BI\Dados\Testes\databases")
    from_db([
            #"[003sql001prd].[db_app]",
            #"[003sql001prd].[db_prod_d0]",
            #"[003sql001prd].[db_prod_d-1]", 
            "[003sql001prd].[db_usuarios]", 
            #"[003sql001prd].[db_Serasa_producao]",
            ],
            generate_creation_migrations=False,             
            overwrite=True,
            base_dir = r"R:\DADOS E BI\Dados\Testes\databases")
    cleanup(target_files="**\\*.sql",
            regex_remove=r"^(\( NAME = N'| ON  | LOG ON).*",
            name_swaps={
                       #"003sql001prd"      : "003sql001dev",
                       "003sql001dev"      : "003sql001prd",
                       #"db_app"            : "db_app_dev",
                       #"db_prod_d0"        : "db_dev_d0",
                       #"db_prod_d-1"       : "db_dev_d-1",
                       "db_usuarios"       : "db_usuarios_dev",
                       #"db_Serasa_producao": "db_Serasa_dev",
                       #"db_producao"       : "db_dev",
                       },
            base_dir=r"R:\DADOS E BI\Dados\Testes\databases")
    schema_to_db([
                #"[003sql001dev].[db_app_dev]",
                #"[003sql001dev].[db_dev_d0]",
                #"[003sql001dev].[db_dev_d-1]", 
                "[003sql001dev].[db_usuarios]", 
                #"[003sql001dev].[db_Serasa_dev]",
                ],
                overwrite=False,
                base_dir=r"R:\DADOS E BI\Dados\Testes\databases")
    migration_to_db("003sql001prd",
                    [
                    "20241001123018-feat-espelhar-views-do-banco-legado-no-banco-de-usuarios", 
                    "20241002120041-fix-corrigir-problema-de-permissionamento-do-setor-de-planejamento-nas-atualizacoes-do-mop",
                    ],
                    base_dir=r"R:\DADOS E BI\Dados\Testes\databases")





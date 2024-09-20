import os
import re
import shutil
import pyodbc
import sqlalchemy as sql
import mssqlscripter.main as scripter


class migration:
    def __init__ (self,
                  base_dir:str,
                  git_user:str,
                  home_server:str,
                  home_db:str,
                  home_repo:str,
                  home_branch:str,
                  ):
        self.git_user:str       = git_user
        self.base_dir:str       = base_dir
        self.home_dir:str       = ""
        self.staging_dir:str    = ""
        self.home_server:str    = home_server
        self.home_db:str        = home_db
        self.home_repo:str      = home_repo
        self.home_branch:str    = home_branch
        self.description:str    = ""
        self.source_server:str  = ""
        self.source_db:str      = ""
        self.source_:str        = ""
        self.sql_conn_str       = None
        self.sql_conn_url       = None
        self.sql_engine         = None
    
        self.start_engine()

    
    def start_engine(self):
        driver_sqls:str = [x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]
        self.sql_conn_str = fr"Driver={{{driver_sqls}}}; Server={self.home_server};Database=master;Trusted_Connection=yes;"
        self.sql_conn_url = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": self.sql_conn_str})
        self.sql_engine = sql.create_engine(self.sql_conn_url, connect_args = {'autocommit':True})
    



    def spawn_from_db(self,
                      source_server:str,
                      source_db:str,
                      swap:str='all'
                      ):
        self.source_server = source_server
        self.source_db = source_db
        self.description = fr"[{self.source_server}].[{self.source_db}] → [{self.home_server}].[{self.home_db}]"
        self.home_dir = fr'{self.base_dir}\{self.description}'
        self.staging_dir = fr"{self.base_dir}\.stg\{self.description.replace(' → ',' __U+2192__ ')}"


        if os.path.isdir(self.staging_dir):
            print(fr"Deleting [{self.staging_dir}]")
            shutil.rmtree(self.staging_dir)
                                               

        print(fr"Trying to connect to {self.source_server}...")
        scripter.main([
            '--connection-string', fr"Server={self.source_server};Database={self.source_db};Trusted_Connection=yes;",
            '-f', fr'{self.staging_dir}\schema' ,
            '--file-per-object',
            '--display-progress'
        ])

        scripter.main([
            '--connection-string', fr"Server={self.source_server};Database={self.source_db};Trusted_Connection=yes;",
            '-f', fr'{self.staging_dir}\_creationscript.sql' ,
            '--display-progress'
        ])


        match swap:
            case 'all':
                for root, _, files in os.walk(self.staging_dir):
                    for file in files:
                        print(fr"Swapping server and db names in [{file}]...")
                        file_path = os.path.join(root, file)
                        text = ''
                        with open(file_path, 'r', encoding='utf-8-sig') as f:
                            text = f.read()
                        text = text.replace(self.source_server, self.home_server)
                        text = text.replace(self.source_db, self.home_db)
                        with open(file_path, 'w', encoding='utf-8') as f:
                            _ = f.write(text)


        if not os.path.isdir(fr"{self.home_dir}\migrations"):
            print(fr"Creating migrations folder...")
            os.makedirs(fr"{self.home_dir}\migrations")
        

        if os.path.isdir(self.home_dir):
            for root, _, files in os.walk(self.home_dir):
                for file in files:
                    if file.endswith(".sql"):
                        print(fr"Deleting {file}...")
                        os.remove(os.path.join(root, file))

    
        for root, _, files in os.walk(self.staging_dir):
            for file in files:
                target_dir = root.replace(self.staging_dir,'')
                full_target_dir = fr"{self.home_dir}\{target_dir}"
                print(fr"Moving {target_dir}\{file}...")
                if not os.path.isdir(full_target_dir):
                    print(fr"Creating {target_dir} folder...")
                    os.makedirs(full_target_dir)
                shutil.move(os.path.join(root, file), os.path.join(full_target_dir, file))


        batches = ''
        with open(fr'{self.home_dir}\_creationscript.sql', 'r', encoding='utf-8') as f:
            batches = re.split(r'(?<=)GO\n', f.read())[:-1]


        with mig.sql_engine.connect() as connection:
            for i, batch in enumerate(batches):
                print('')
                print(fr"GO - - - > Batch {i+1}/{len(batches)}")
                print(batch)
                _ = connection.execute(sql.text(batch))


        print('Migration spawned!') 



    #TODO: commit_to_branch(self, target_repo, target_branch)
        #TODO: upload to home branch
        #TODO: upload to target branch (with request)
    


if __name__.endswith('__main__'):


    mig = migration(
        base_dir = r'R:\DADOS E BI\Dados\Testes\databases',
        git_user = r'andi',
        home_server = r'000sql000prd',
        home_db = r'db_dev',
        home_repo = r'TestCompany/databases',
        home_branch = r'CICD Testing - Dev',
    )

    mig.spawn_from_db(
        source_server = r'000sql000prd',
        source_db = r'db_prod',
    )

    #mig.commit_to_branch(
    #    target_repo = r'TestCompany/databases',
    #    target_branch = r'CICD Testing - Prod',
    #)





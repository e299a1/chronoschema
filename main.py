import os
import re
import shutil
import pyodbc
import urllib3
import sqlalchemy as sql
import mssqlscripter.main as scripter
import git
import github

class migration:
    def __init__ (self,
                  base_dir:str,
                  gh_pat:str,
                  home_server:str,
                  home_db:str,
                  home_repo:str,
                  home_branch:str,
                  ):
        self.gh_pat:str             = gh_pat
        self.base_dir:str           = base_dir
        self.home_dir:str           
        self.staging_dir:str        
        self.home_server:str        = home_server
        self.home_db:str            = home_db
        self.home_repo:str          = home_repo
        self.home_branch:str        = home_branch
        self.description:str        
        self.source_server:str      
        self.source_db:str          
        self.source_:str            
        self.sql_driver:str         = [x for x in pyodbc.drivers() if x.endswith('SQL Server')][0]         
        self.sql_conn_str:str       = fr"Driver={{{self.sql_driver}}}; Server={self.home_server};Database=master;Trusted_Connection=yes;"
        self.sql_conn_url:sql.URL   = sql.engine.URL.create("mssql+pyodbc", query={"odbc_connect": self.sql_conn_str})        
        self.sql_engine:sql.Engine  = sql.create_engine(self.sql_conn_url, connect_args = {'autocommit':True})
    

    def spawn_from_db(self,
                      source_server:str,
                      source_db:str,
                      swap:str='all',
                      overwrite:bool=False,
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
            '--script-create',
            #'--change-tracking',
            '--exclude-headers',
            '--exclude-defaults',
            '--display-progress',
        ])

        scripter.main([
            '--connection-string', fr"Server={self.source_server};Database={self.source_db};Trusted_Connection=yes;",
            '-f', fr'{self.staging_dir}\_creationscript.sql' ,
            '--script-create',
            #'--change-tracking',
            '--exclude-headers',
            '--exclude-defaults',
            '--display-progress',
        ])


        match swap:
            case 'all':
                for root, _, files in os.walk(self.staging_dir):
                    for file in files:
                        print(fr"Swapping server and db names in [{file}]...")
                        file_path = os.path.join(root, file)
                        with open(file_path, 'r', encoding='utf-8-sig') as f:
                            text = f.read()
                        if file == "_creationscript.sql" or file == fr"{self.source_db}.Database.sql":
                            print(fr"Removing DB filesize info in [{file}]...")
                            text = re.sub(r"^(\( NAME = N'| ON  | LOG ON).*", "", text, flags=re.MULTILINE)
                        text = text.replace(self.source_server, self.home_server)
                        text = text.replace(self.source_db, self.home_db)
                        with open(file_path, 'w', encoding='utf-8') as f:
                            _ = f.write(text)
         


        if not os.path.isdir(fr"{self.home_dir}\migrations"):
            print(fr"Creating migrations folder...")
            os.makedirs(fr"{self.home_dir}\migrations")
        

        if os.path.isdir(self.home_dir) and overwrite:
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


        with open(fr'{self.home_dir}\_creationscript.sql', 'r', encoding='utf-8') as f:
            batches = re.split(r"(?<=)GO\n", f.read())[:-1]

        
        with self.sql_engine.connect() as connection:
            if overwrite:
                _ = connection.execute(sql.text(fr"DROP DATABASE IF EXISTS [{self.home_db}];"))

            for i, batch in enumerate(batches):
                try:
                    _ = connection.execute(sql.text(batch))
                    print(fr"GO - - - > Batch {i+1}/{len(batches)} OK!")
                except Exception as exc:
                    print(exc)
                    print(fr"XX - - - > Batch {i+1}/{len(batches)} FAILED!")


        try:
            _ = git.Repo(self.home_dir)
            print(fr"The local Git repository {self.home_repo} already exists.")
            if overwrite:
                print("Clearing existing local Git repository...")
                shutil.rmtree(fr"{self.home_dir}\.git", ignore_errors=True)
        except:
            pass


        print("Initializing new local Git repository...")
        repo = git.Repo.init(self.home_dir)
        _ = repo.git.branch("-M", self.home_branch)
        _ = repo.git.add("--all")
        _ = repo.git.commit("-m", "Initial commit")

        
        ghub = github.Github(self.gh_pat, verify=False)
        entity = ghub.get_user() #entity = ghub.get_organization("org-name")

        try:
            existing_ghrepo = entity.get_repo(self.home_repo)
            print(fr"The GitHub repository {self.home_repo} already exists.")
            if overwrite:
                print("Deleting existing GitHub repository...")
                existing_ghrepo.delete()
        except:
            pass

        print("Initializing new GitHub repository...")
        ghrepo = entity.create_repo(
                    name=self.home_repo,
                    allow_rebase_merge=True,
                    auto_init=False,
                    description=self.description,
                    has_issues=True,
                    has_projects=False,
                    has_wiki=False,
                    private=True,
                 )
        _ = repo.git.remote("add", "origin", fr"https://github.com/{ghrepo.full_name}.git")
        _ = repo.git.push("-u", "origin", self.home_branch)


        print('Migration spawned!') 

    #def commit_to_branch(self, target_repo:str, target_branch:str):
    #    #TODO: commit/push to home branch without pull request by default
    #    #TODO: commit/push to target branch with pull request
    


if __name__.endswith('__main__'):
    urllib3.disable_warnings()

    mig = migration(
        base_dir = r'R:\DADOS E BI\Dados\Testes\databases',
        gh_pat = "ghp_xxx",
        home_server = r'000sql000prd',
        home_db = r'db_dev',
        home_repo = r'databases',
        home_branch = r'CICD_Testing_Dev',
    ).spawn_from_db(
        source_server = r'000sql000prd',
        source_db = r'db_prod',
        overwrite = True,
    )
    #.commit_to_branch(
    #    target_repo = r'TestCompany/databases',
    #    target_branch = r'CICD_Testing_Prod',
    #)

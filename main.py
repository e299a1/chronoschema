import os
import shutil
import mssqlscripter.main as scripter


class migration:
    def __init__ (self, base_dir, git_user, home_server, home_db, home_repo, home_branch):
        # prod_db -> dev_db -> dev_repo -> prod_repo

        self.git_user = git_user
        self.base_dir = base_dir
        self.home_server = home_server
        self.home_db = home_db
        self.home_repo = home_repo
        self.home_branch = home_branch


    def spawn_from_db(self, source_server, source_db, swap='all'):
        # prod_db -> dev_db

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
            '-f', self.staging_dir ,
            '--file-per-object',
            '--display-progress'
        ])


        match swap:
            case 'all':
                for root, dirs, files in os.walk(self.staging_dir):
                    for file in files:
                        print(fr"Swapping server and db names in [{file}]...")
                        file_path = os.path.join(root, file)
                        text = ''
                        with open(file_path) as f:
                            text = f.read()
                        text = text.replace(self.source_server, self.home_server)
                        text = text.replace(self.source_db, self.home_db)
                        with open(file_path, "w") as f:
                            f.write(text)


        if os.path.isdir(self.home_dir):
            for root, dirs, files in os.walk(self.home_dir):
                for file in files:
                    if file.endswith(".sql"):
                        print(fr"Deleting [{file}]...")
                        os.remove(os.path.join(root, file))

    
        for root, dirs, files in os.walk(self.staging_dir):
            for file in files:
                print(fr"Moving [{file}]...")
                shutil.move(os.path.join(root, file), self.home_dir)


        if not os.path.isdir(fr"{self.home_dir}\migrations"):
            print(fr"Creating migrations folder...")
            os.makedirs(fr"{self.home_dir}\migrations")


        #TODO: run creationscripts


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





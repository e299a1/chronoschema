import os
import mssqlscripter.main as scripter


class migration:
    def __init__ (self, base_dir, git_user, home_server, home_db, home_repo, home_branch):
        # prod_db -> dev_db -> dev_repo -> prod_repo

        self.git_user = git_user
        self.base_dir = base_dir
        self.home_dir = fr'{base_dir}\{source_server}.{source_db} - {home_repo.replace(r'/', '-')}.{home_branch} - {target_repo.replace(r'/', '-')}.{target_branch}'
        self.home_server = home_server
        self.home_db = home_db
        self.home_repo = home_repo
        self.home_branch = home_branch

        if not os.path.isdir(fr"{self.home_dir}\migrations"):
            os.makedirs(fr"{self.home_dir}\migrations")
            

    def spawn_from_db(self, source_server, source_db):
        # prod_db -> dev_db

        self.source_server = source_server
        self.source_db = source_db

        if os.path.isdir(self.home_dir):
            for item in os.listdir(self.home_dir):
                if item.endswith(".sql"):
                    print(fr"Deleting [{item}]")
                    os.remove(fr"{self.home_dir}\{item}")

        print(fr"Trying to connect to {self.source_server}...")
        scripter.main([
            '--connection-string', fr"Server={self.source_server};Database={self.source_db};Trusted_Connection=yes;",
            '-f', self.home_dir,
            '--file-per-object',
            '--display-progress'
        ])

        #TODO: modify scripts
        #TODO: run scripts

    #TODO: commit_to_branch(self, target_repo, target_branch)
        #TODO: upload to home branch
        #TODO: upload to target branch (with request)



    


if __name__.endswith('__main__'):

    mig = migration(
        base_dir = r'R:\DADOS E BI\Dados\Testes\databases',
        git_user = r'andi',
        home_server = r'003sql001prd',
        home_db = r'db_dev_d-1',
        home_repo = r'GrupoSelecionar/databases',
        home_branch = r'CICD Testing - Dev',
    )

    mig.spawn_from_db(
        source_server = r'003sql001prd',
        source_db = r'db_prod_d-1',
    )

    #mig.commit_to_branch(
    #    target_repo = r'GrupoSelecionar/databases',
    #    target_branch = r'CICD Testing - Prod',
    #)





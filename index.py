import os, os.path, sys
from cmd import Cmd
from src.deleter import deleter

class main(Cmd):

    def __init__(self):
        super(main, self).__init__()

    def do_ingest(self, args):
        table, projectKey = args.split(" ", 2)
        print(f"Deleting '{projectKey}' from table '{table}'")
        deleter.rowDeleter(table,dev=False, ProjectKey=f"{projectKey}")

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: delete, exit.")

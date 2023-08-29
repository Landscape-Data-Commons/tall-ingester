import os, os.path, sys
from cmd import Cmd
from configparser import ConfigParser
import src.utils.tables as tbl

os.getcwd()
config = ConfigParser()
inipath = os.path.join(os.getcwd(),"src","utils","database.ini")
config.read(inipath)
inistr = ", ".join(config.sections())
class main(Cmd):

    def __init__(self):
        super(main, self).__init__()
        self.prompt = "> "
        self.batch_path = os.path.normpath(os.path.join(os.getcwd(),"talltables"))
        self.dimafiles = [os.path.normpath(f"{self.batch_path}/{i}") for i in os.listdir(self.batch_path)]


    def do_ingest(self, args):
        database = args.split(" ", 1)
        print(f"currently ingesting to '{database[0]}' ")
        tbl.batcher(database[0])
        # deleter.rowDeleter(table,dev=False, ProjectKey=f"{projectKey}")

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop(f"available database sources: {inistr}")
    app.cmdloop("Currently available commands: ingest, exit.")

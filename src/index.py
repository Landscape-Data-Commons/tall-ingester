import os, os.path, sys
from cmd import Cmd
import src.utils.batchutils as butils

class main(Cmd):

    def __init__(self):
        super(main, self).__init__()

    def do_ingest(self, whichschema):
        print(f"Ingesting!")
        butils.batch()

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: delete, exit.")

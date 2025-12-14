#!/usr/bin/env python
import os
import sys
import json
import argparse

from time   import sleep



def run():

    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser = argparse.ArgumentParser()

    parser.add_argument('-j','--job', action='store', dest='job', required = True,
                        help = "The job input")
    parser.add_argument('-o','--output', action='store', dest='output', required = False, default='circuit.json',
                        help = "The job output")

    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    basepath = os.getcwd()

    with open( args.job , 'r') as f:
        d = json.load(f)
        result = d['input']
        
    sleep(0.5) # simulate some work being done
    o = {'input': result}
    print(f"saving into {args.output}...")
    with open(args.output , 'w') as f:
        json.dump(o, f)

    sys.exit(0)

if __name__ == "__main__":
    run()
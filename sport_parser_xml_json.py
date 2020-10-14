# -*- coding: utf-8 -*-
import argparse
import io
from datetime import datetime
import glob
import json
import locale

locale.setlocale(locale.LC_ALL, 'nl_NL')
import os
import os.path as path
import sys
import unicodedata
import xmltodict


def stderr(text=""):
    sys.stderr.write("{}\n".format(text))


def arguments():
    ap = argparse.ArgumentParser(description='Read and convert files from "sportverenigingen"')
    ap.add_argument('-d', '--inputdir',
                    help="inputdir",
                    default="/Users/vic/Documents/sportsbank/data")
    ap.add_argument('-o', '--outputdir',
                    default="/Users/vic/Documents/sportsbank/jsondata",
                    help="outputdir")
    args = vars(ap.parse_args())
    return args


if __name__ == "__main__":
    stderr("start: {}".format(datetime.today().strftime("%H:%M:%S")))

    args = arguments()
    inputdir = args['inputdir']
    outputdir = args['outputdir']

    all_files = glob.glob("{}/**/*.xml".format(inputdir))
    counter = 0

    for f in all_files:
        counter += 1
        with io.open(f, encoding='utf-8') as fh:
            current_dir = f.replace(inputdir, '')[1:] if f.replace(inputdir, '')[0:1] == '/' else f.replace(inputdir, '')
            outputpath_with_filename = path.join(outputdir, current_dir)
            # make output path ready
            outputpath = path.dirname(outputpath_with_filename)
            if not path.isdir(outputpath):
                os.makedirs(outputpath)
            outputfilename_without_extension, _ = path.splitext(outputpath_with_filename)
            # make output path + filename ready
            full_outputpath = outputfilename_without_extension + '.json'
            doc = xmltodict.parse(fh.read())
            json_out = io.open(full_outputpath, "w", encoding='utf-8')
            json_out.write(json.dumps(doc, sort_keys=False, indent=2, ensure_ascii=False))

    stderr('{} files read'.format(counter))
    stderr("end: {}".format(datetime.today().strftime("%H:%M:%S")))

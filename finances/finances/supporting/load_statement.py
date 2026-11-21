import argparse

from utils import HSBCFile, ToptalFile

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    parser.add_argument("--volume", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    args = parser.parse_args()

    HSBCFile(args.catalog, args.schema, args.volume, args.table).process()
    ToptalFile(args.catalog, args.schema, args.volume, args.table).process()

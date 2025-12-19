#!/usr/bin/env python3
import argparse
import logging

import yaml
from postgresql_access import DatabaseDict

from periodic_database_jobs import periodic_db_logger, run_jobs


def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")
    parser.add_argument('yaml',help='Configuration')

    args = parser.parse_args()
    periodic_db_logger.setLevel(getattr(logging, args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load(f)
    db = DatabaseDict(dictionary=config['database'])
    with db.connect(application_name="periodic db jobs") as conn:
        run_jobs(config,conn)



if __name__ == "__main__":
    main()


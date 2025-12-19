#!/usr/bin/env python3
import argparse
import logging
from periodic_database_jobs import periodic_database_jobs_logger


def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")

    args = parser.parse_args()
    periodic_database_jobs_logger.setLevel(getattr(logging,args.loglevel))


if __name__ == "__main__":
    main()


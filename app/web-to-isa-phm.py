from __future__ import annotations

import argparse
import json
import logging

from isatools.isajson import ISAJSONEncoder

from converter.entrypoint import create_isa_data


def convert_file(input_path: str, output_path: str) -> None:
    logger = logging.getLogger("isa_phm_converter")
    with open(input_path, "r", encoding="utf-8-sig") as infile:
        payload = json.load(infile)

    logger.info("Loading ISA-PHM JSON file: %s", input_path)
    investigation = create_isa_data(isa_phm_info=payload, output_path=output_path, logger=logger)

    with open(investigation.filename, "w", encoding="utf-8", newline="\n") as outfile:
        json.dump(
            investigation,
            outfile,
            cls=ISAJSONEncoder,
            sort_keys=True,
            indent=4,
            separators=(",", ": "),
        )

    logger.info("ISA-PHM JSON file created: %s", investigation.filename)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="Input JSON file that contains the information needed to create ISA-PHM output",
    )
    parser.add_argument(
        "outfile",
        help="Output file name for the ISA-PHM JSON file",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    convert_file(args.file, args.outfile)


if __name__ == "__main__":
    main()

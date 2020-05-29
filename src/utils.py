import mlspeclib
import logging
from logging import LogRecord
from io import StringIO
import sys
import os
from pathlib import Path

if Path("src").exists():
    sys.path.append(str(Path("src")))
sys.path.append(str(Path.cwd()))
sys.path.append(str(Path.cwd().parent))


class KnownException(Exception):
    pass


def report_found_params(expected_params: list, offered_params: dict) -> None:
    rootLogger = setupLogger().get_root_logger()
    for param in expected_params:
        if param not in offered_params or offered_params[param] is None:
            raise KnownException(f"No parameter set for {param}.")
        else:
            rootLogger.debug(f"Found value for {param}.")


def raise_schema_mismatch(
    expected_type: str, expected_version: str, actual_type: str, actual_version: str
):
    raise KnownException(
        f"""Actual data does not match the expected schema and version:
    Expected Type: {expected_type}
    Actual Type: {actual_type}

    Expected Version: {expected_version}
    Actual Version: {actual_version}")"""
    )


# TODO: Think about moving logger to a library of some kind so that it can be reused with this signature across derivaed containers
class setupLogger:
    _rootLogger = None
    _buffer = None

    def __init__(self):
        # logging.config.fileConfig('logging.conf')

        # return (logger, None)
        self._rootLogger = logging.getLogger()
        self._rootLogger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("::%(levelname)s - %(message)s")

        if not self._rootLogger.hasHandlers():
            self._buffer = StringIO()
            bufferHandler = logging.StreamHandler(self._buffer)
            bufferHandler.setLevel(logging.DEBUG)
            bufferHandler.setFormatter(formatter)
            bufferHandler.set_name("buffer.logger")
            self._rootLogger.addHandler(bufferHandler)

            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(logging.DEBUG)
            stdout_handler.setFormatter(formatter)
            stdout_handler.set_name("stdout.logger")
            self._rootLogger.addHandler(stdout_handler)

            set_output_handler = logging.StreamHandler(sys.stdout)
            set_output_handler.setLevel(logging.NOTSET)
            set_output_handler.setFormatter(logging.Formatter("%(message)s"))
            set_output_handler.addFilter(self.filter_for_outputs)
            set_output_handler.set_name("setoutput.logger")
            self._rootLogger.addHandler(set_output_handler)
        else:
            for i, handler in enumerate(self._rootLogger.handlers):
                if handler.name == "buffer.logger":
                    self._buffer = self._rootLogger.handlers[i].stream
                    break

            if self._buffer is None:
                raise SystemError(
                    "Somehow, we've lost the 'buffer' logger, meaning nothing will be printed. Exiting now."
                )

    def get_loggers(self):
        return (self._rootLogger, self._buffer)

    def get_root_logger(self):
        return self._rootLogger

    def get_buffer(self):
        return self._buffer

    def print_and_log(self, variable_name, variable_value):
        # echo "::set-output name=time::$time"
        output_message = f"::set-output name={variable_name}::{variable_value}"
        print(output_message)
        self._rootLogger.critical(output_message)

        return output_message

    @staticmethod
    def filter_for_outputs(record: LogRecord):
        if str(record.msg).startswith("::set-output"):
            return True
        return False

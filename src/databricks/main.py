'''
Entry point script used by all pipeline runs
'''

import argparse
from importlib import import_module
from pyspark.sql import SparkSession

def add_args(parser: argparse.ArgumentParser):
    '''Generally arguments are given when the pipeline is run
    But for easier debugging, values can be set here and the code run directly

    Args:
        parser (argparse.ArgumentParser): may contain command line arguments
    '''
    parser.add_argument('--job_path', nargs='?')
    parser.add_argument('--file_path', nargs='?')
    parser.add_argument('--cleansed_date_format', nargs='?')
    parser.add_argument('--workspace_folder', nargs='?')

    return parser

def execute_job(job_path: str, file_path: str, cleansed_date_format: str, workspace_folder: str):
    '''Calls the execute function in the job file identified by the job_path

    Args:
        job_path: path to the job python file to have execute function called
        example_arg: used as an example for passing an argument into the executed function
    '''
    job_module = job_path.split('.')[0]
    module_path = 'jobs/' + job_module
    module_path = module_path.replace('/', '.')
    module = import_module(module_path)
    execute_function = getattr(module, 'execute')
    execute_function(file_path, cleansed_date_format, workspace_folder)

arg_parser = argparse.ArgumentParser()
arg_parser = add_args(arg_parser)
arguments = arg_parser.parse_args()
execute_job(job_path=arguments.job_path, file_path=arguments.file_path, cleansed_date_format=arguments.cleansed_date_format, workspace_folder=arguments.workspace_folder)
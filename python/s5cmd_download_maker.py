""" Runs the s3 download stuff:

	Args are just:
	- s3 directory to download
	- local directory to save it to 
	- part id (defaults to 0)
	- num_parts (defaults to 1)
"""


import click
import subprocess
import sys
import threading
import boto3
from urllib.parse import urlparse
from tempfile import NamedTemporaryFile

@click.group()
def cli():
	pass



# ==================================================
# =            THINGS I STOLE FROM CLAUDE          =
# ==================================================
# Idk what any of these things do, I just asked claude for help lol -mj


def stream_output(process):
    """Stream process output in real-time to stdout and stderr.
		
    """
    def stream_pipe(pipe, std_pipe):
        for line in iter(pipe.readline, b''):
            std_pipe.write(line.decode())
            std_pipe.flush()  # Important: flush immediately
    
    # Create threads for stdout and stderr
    stdout_thread = threading.Thread(
        target=stream_pipe,
        args=(process.stdout, sys.stdout),
        daemon=True
    )
    stderr_thread = threading.Thread(
        target=stream_pipe,
        args=(process.stderr, sys.stderr),
        daemon=True
    )
    
    # Start threads
    stdout_thread.start()
    stderr_thread.start()
    
    # Wait for process to complete
    return_code = process.wait()
    
    # Wait for threads to finish
    stdout_thread.join()
    stderr_thread.join()
    
    return return_code

def run_command(command):
    """
    Run a bash command with real-time output streaming to stdout/stderr
    and proper error handling.
    
    Args:
        command: String representing the command to run
        
    Returns:
        Return code of the process
        
    Raises:
        CalledProcessError: If the command returns a non-zero exit code

    """
    # Start the process
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,  # Unbuffered
        universal_newlines=False  # Use bytes for maximum compatibility
    )
    
    # Stream output and get return code
    return_code = stream_output(process)
    
    # Raise exception if command failed
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    
    return return_code



def list_s3_files(s3_uri):
    """
    Lists all files under a specific S3 prefix.
    
    Args:
        s3_uri (str): The S3 URI in the format "s3://bucket-name/prefix/path"
        
    Returns:
        list: A list of S3 object keys (file paths)
    """
    # Parse the S3 URI
    parsed_uri = urlparse(s3_uri)
    bucket_name = parsed_uri.netloc
    prefix = parsed_uri.path.lstrip('/')
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # List objects with the specified prefix
    file_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Paginate through results (in case there are more than 1000 files)
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                file_list.append(obj['Key'])
    
    return file_list

# ===================================================
# =                     S5CMD BINDINGS              =
# ===================================================

@cli.command()
@click.option('--src', required=True)
@click.option('--dst', required=True)
@click.option('--part', type=int, default=0)
@click.option('--num-parts', type=int, default=1)
def download(src, dst, part, num_parts):
	assert part < num_parts
	if num_parts == 1:
		# Just run the `s5cmd cp command directly`
		command = 's5cmd cp -sp %s %s' % (src, dst)
	else:
		# Create a text file to run `s5cmd run ...` on 

		# So first get the list of files and sort them
		files_to_download = sorted(list_s3_files(src))[part::num_parts]
		# Then create a temp file with the files to download
		f = NamedTemporaryFile('w')
		for filename in files_to_download:
			f.write('cp %s %s\n' % (filename, dst))
		f.flush()

		# and build the s5cmd and cleanup
		command = 's5cmd run %s' % f.name

	run_command(command)
	


@cli.command()
@click.option('--src', required=True)
@click.option('--dst', required=False)
def upload(src, dst):
	# Is literally just an s5cmd wrapper for uploading

	command = 's5cmd cp -sp %s %s' % (src, dst)
	run_command(command)





# ====================================================
# =                       MAIN                       =
# ====================================================

if __name__ == '__main__':
	cli()
import fcntl
import io
import os
import re
import select
import subprocess
import sys
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple, Union

import boto3
import click
from tqdm.auto import tqdm


@click.group()
def cli():
    pass


# =====================================================
# =                   S5CMD WRAPPERS                  =
# =====================================================


def set_non_blocking(file_obj):
    """Set file object to non-blocking mode"""
    fd = file_obj.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def weka_endpoint_args(weka_profile=None):
    if weka_profile == None:
        weka_profile = "WEKA"
    cmd_str = (
        "--profile %s --endpoint-url https://weka-aus.beaker.org:9000" % weka_profile
    )
    return cmd_str.split(" ")


class S5CMD:
    """
    Python bindings for s5cmd
    """

    def __init__(
        self,
        binary_path: Optional[str] = None,
    ):
        """
        Initialize the S5CMD wrapper

        Args:
            binary_path: Path to the s5cmd binary. If None, the binary is expected to be in PATH
        """
        self.binary_path = binary_path or "s5cmd"

        # Verify the binary exists and is executable
        self._verify_binary()

    def _verify_binary(self):
        """Verify that the s5cmd binary exists and is executable"""
        try:
            result = subprocess.run(
                [self.binary_path, "help"], capture_output=True, text=True, check=True
            )
            # Optional: print version information
            # print(f"Using s5cmd version: {result.stdout.strip()}")
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            raise RuntimeError(f"Failed to execute s5cmd binary: {e}")

    def cp(
        self,
        source: str,
        destination: str,
        include: Optional[str] = None,
        exclude: Optional[str] = None,
        weka_profile: Optional[str] = None,
    ) -> int:
        """
        Copy files from source to destination

        Args:
            source: Source path (s3:// or local)
            destination: Destination path (s3:// or local)
            show_progress: Whether to show progress bar (overrides instance setting)
            include: Include pattern
            exclude: Exclude pattern

        Returns:
            Return code from s5cmd
        """

        # Build command
        cmd = [self.binary_path, "cp", "-sp"]

        if include:
            cmd.extend(["--include", include])

        if exclude:
            cmd.extend(["--exclude", exclude])

        if any("weka://" in [source, destination]):
            cmd.extend(weka_endpoint_args(weka_profile))
            source = source.replace("weka://", "s3://")
            destination = destination.replace("weka://", "s3://")

        assert not all(
            "s3://" in [source, destination]
        ), "Only s3/weka<->local permitted!"

        # some checks:

        cmd.extend([source, destination])

        pbar = tqdm(
            total=100, unit="%", bar_format="{l_bar}{bar}| {n:.2f}/{total}% {postfix}"
        )

        # Execute command
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,  # text mode
            bufsize=0,  # Unbuffered
        )

        set_non_blocking(process.stdout)

        # Process output with poll/select mechanism
        try:
            running = True
            poller = select.poll()
            poller.register(process.stdout, select.POLLIN)
            while running:
                # Wait for data (100ms timeout)
                if poller.poll(100):
                    try:
                        data = process.stdout.read()
                        if not data:
                            # End of stream
                            if process.poll() is not None:
                                running = False
                                continue
                        if not data.strip():
                            continue
                        self._update_cp_progress_bar(pbar, data.strip().split())

                    except io.BlockingIOError:
                        # No data available right now
                        pass
                # Check if process has finished
                if process.poll() is not None:
                    running = False

            # Wait for process to complete
            return_code = process.wait()
            return return_code

        except KeyboardInterrupt:
            # Try to gracefully terminate the process
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()

            if progress_bar:
                progress_bar.finish()

            raise
        finally:
            # Make sure we clean up the process
            if process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=2)
                except (subprocess.TimeoutExpired, OSError):
                    try:
                        process.kill()
                    except OSError:
                        pass

    @classmethod
    def _update_cp_progress_bar(cls, pbar, datasplit):
        pct = datasplit[0]
        if "?" in pct:
            return
        postfix = " ".join(datasplit[2:])
        pct = float(pct.replace("%", ""))
        delta = pct - pbar.n
        # print("PCT", pct)
        # print("POSTFIX", postfix)

        pbar.update(round(delta, 2))
        pbar.set_postfix_str(postfix)

    def ls(self, path: str, recursive: bool = False, weka_profile=None) -> List[Dict]:
        """
        List files and objects

        Args:
            path: Path to list (s3:// or local)
            recursive: Whether to list recursively

        Returns:
            List of objects/files with their details
        """
        cmd = [self.binary_path]

        if recursive:
            cmd.append("--recursive")

        if path.startswith("weka://"):
            path = path.replace("weka://", "s3://")
            cmd.extend(weka_endpoint_args(weka_profile))

        cmd.append("ls")
        cmd.append(path)
        print("RUNNING", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Parse the output
        files = []
        while os.path.basename(path) and "*" in os.path.basename(path):
            path = os.path.dirname(path)

        for line in result.stdout.strip().split("\n"):
            if not line:
                continue

            parts = line.split()
            if len(parts) >= 4:  # Date Size Time Filename
                size = int(parts[2])
                filename = " ".join(parts[3:])
                files.append(
                    {
                        "size": size,
                        "name": os.path.join(path, filename),
                    }
                )

        return files

    def run(self, cmd_file, cp_pbar=True):
        cmd = [self.binary_path, "run", cmd_file]
        if cp_pbar:
            f = open(cmd_file, "r")
            # cmd_file.seek(0)
            num_lines = sum(1 for _ in f if _.startswith("cp"))
            pbar = tqdm(total=num_lines, unit="Files")
            inc_pbar = lambda line: pbar.update(int(line.startswith("cp")))
        else:
            inc_pbar = lambda line: None

        errs = []
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,  # text mode
            bufsize=0,  # Unbuffered
        )

        set_non_blocking(process.stdout)

        # Process output with poll/select mechanism
        try:
            running = True
            poller = select.poll()
            poller.register(process.stdout, select.POLLIN)
            while running:
                # Wait for data (100ms timeout)
                if poller.poll(100):
                    try:
                        data = process.stdout.read()
                        if not data:
                            # End of stream
                            if process.poll() is not None:
                                running = False
                                continue
                        if not data.strip():
                            continue

                        inc_pbar(data)
                        if data.startswith("ERROR "):
                            errs.append(data)
                        if (
                            cp_pbar
                            and data.strip()
                            and not data.strip().startswith("cp")
                        ):
                            print(data)

                    except io.BlockingIOError:
                        # No data available right now
                        pass
                # Check if process has finished
                if process.poll() is not None:
                    running = False

            # Wait for process to complete
            return_code = process.wait()

        except KeyboardInterrupt:
            # Try to gracefully terminate the process
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()

            if progress_bar:
                progress_bar.finish()

            raise
        finally:
            # Make sure we clean up the process
            if process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=2)
                except (subprocess.TimeoutExpired, OSError):
                    try:
                        process.kill()
                    except OSError:
                        pass

        return errs


# =========================================================
# =                           CLI STUFF                   =
# =========================================================


@cli.command()
@click.option("--src", required=True)
@click.option("--dst", required=True)
@click.option("--part", type=int, default=0)
@click.option("--num-parts", type=int, default=1)
def download(src, dst, part, num_parts):
    assert part < num_parts

    s5 = S5CMD()
    if num_parts == 1:
        # Just run the `s5cmd cp command directly`
        s5.cp(src, dst)
    else:
        # Create a text file to run `s5cmd run ...` on

        # So first get the list of files and sort them
        s3_files = [_["name"] for _ in s5.ls(src)]
        files_to_download = sorted(s3_files)[part::num_parts]
        # Then create a temp file with the files to download
        f = NamedTemporaryFile("w")
        for filename in files_to_download:
            f.write("cp %s %s\n" % (filename, dst))
        f.flush()
        s5.run(f.name, cp_pbar=True)


@cli.command()
@click.option("--src", required=True)
@click.option("--dst", required=False)
def upload(src, dst):
    # Is literally just an s5cmd wrapper for uploading
    s5 = S5CMD()
    s5.cp(src, dst)


if __name__ == "__main__":
    cli()

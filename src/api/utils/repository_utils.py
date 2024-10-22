import random
import string
from github import Github
from github import Auth
from packaging import version
import os
import shutil
import requests
import tarfile
from loguru import logger
import logging

def get_newer_release_version(token, repository, existing_version):
    """
    Checks for a newer release version in the specified GitHub repository.

    Parameters:
        token (str): The GitHub authentication token.
        repository (str): The repository in the format "owner/repo".
        existing_version (str): The currently installed version.

    Returns:
        str: The tag name of the newer release if available, otherwise None.
    """
    auth = Auth.Token(token)
    g = Github(auth = auth)
    logger.debug("Looking for repo: {0} with token {1}".format(repository, token))
    repo = g.get_repo(repository)
    
    last_version = repo.get_latest_release()
    if (existing_version is None or version.parse(last_version.tag_name)>version.parse(existing_version)):
        logger.info("New version detected: {0}".format(last_version))
        return last_version.tag_name
    else:
        logger.info("No new version detected")
        return None
    
def get_worker_installed_version(version_path):
    """
    Retrieves the currently installed version of the worker from a file.

    Parameters:
        version_path (str): The path to the file containing the installed version.

    Returns:
        str: The installed version if the file exists and is readable, otherwise None.
    """
    try:
        with open(version_path,"r") as file:
            current_version = file.readline()
            return current_version
    except:
        return None
    
def set_worker_installed_version(version_path, new_version):
    """
    Sets the installed version of the worker in a file.

    Parameters:
        version_path (str): The path to the file where the version will be written.
        new_version (str): The new version to be set.
    """
    with open(version_path,"w+") as file:
        version = file.writelines([new_version])

def create_dir_if_not_exists(dir_path):
    """
    Creates a directory if it does not already exist.

    Parameters:
        dir_path (str): The path of the directory to create.
    """

    isExist = os.path.isdir(dir_path)
    if not isExist:
        os.makedirs(dir_path)

def remove_and_recreate_dir(dir_path):
    """
    Removes a directory if it exists and then recreates it.

    Parameters:
        dir_path (str): The path of the directory to remove and recreate.
    """
    isExist = os.path.isdir(dir_path)
    if isExist:
        shutil.rmtree(dir_path)
    create_dir_if_not_exists(dir_path)

def download_tar_from_github(tar_url, token, destination_file):
    """
    Downloads a tar file from a GitHub URL.

    Parameters:
        tar_url (str): The URL of the tar file to download.
        token (str): The GitHub authentication token.
        destination_file (str): The path where the downloaded file will be saved.
    """
    headers = {"Authorization": "Bearer " + token}
    response = requests.get(tar_url, headers = headers)
    target_dir = os.path.dirname(destination_file)
    create_dir_if_not_exists(target_dir)
    open(destination_file, "wb").write(response.content)

def download_and_extract_new_release(token, repository,target_version, install_path, 
                                     tmp_dir = "/tmp/latest_worker_temp", tmp_download_file="/tmp/latest_worker.tar", 
                                     installed_version_file_name = "installed_version.txt"):
    """
    Downloads and extracts a new release from a GitHub repository.

    Parameters:
        token (str): The GitHub authentication token.
        repository (str): The repository in the format "owner/repo".
        target_version (str): The version tag to download.
        install_path (str): The path where the release will be installed.
        tmp_dir (str): The temporary directory for extraction.
        tmp_download_file (str): The temporary file path for the downloaded tar file.
        installed_version_file_name (str): The name of the file that will store the installed version.

    Returns:
        bool: True if the new version was successfully installed, otherwise False.
    """
    try:
        auth = Auth.Token(token)
        g = Github(auth = auth)
        repo = g.get_repo(repository)
        release = repo.get_release(target_version)
        tar_url = release.tarball_url
        a =list(repo.get_tags())
        sha = [x.commit.sha for x in a if x.name==target_version]
        sha = sha[0]
        download_and_extract_new_tar(token, tar_url, sha, install_path, tmp_dir , tmp_download_file)
        set_worker_installed_version(os.path.join(install_path, installed_version_file_name), target_version)
        print("New version installed: {0}".format(target_version))
        return True
    except Exception as e:
        print("Error: the new version could not be installed")
        return False
    
def download_and_extract_branch(token, repository, branch, install_path, 
                                     tmp_dir = "/tmp/latest_worker_temp", tmp_download_file="/tmp/latest_worker.tar"):
    """
    Downloads and extracts a branch from a GitHub repository.

    Parameters:
        token (str): The GitHub authentication token.
        repository (str): The repository in the format "owner/repo".
        branch (str): The branch name to download.
        install_path (str): The path where the branch will be installed.
        tmp_dir (str): The temporary directory for extraction.
        tmp_download_file (str): The temporary file path for the downloaded tar file.

    Returns:
        bool: True if the branch was successfully downloaded and extracted, otherwise False.
    """
    try:
        auth = Auth.Token(token)
        g = Github(auth = auth)
        repo = g.get_repo(repository)
        branch_obj = repo.get_branch(branch)
        tar_url = "https://api.github.com/repos/" + repository + "/tarball/" + branch
        sha = branch_obj.commit.sha
        download_and_extract_new_tar(token, tar_url, sha, install_path, tmp_dir , tmp_download_file)
        print("Model downloaded: {0}".format(branch))
        return True
    except Exception as e:
        print("Error: the model version could not be downloaded")
        return False

def download_and_extract_new_tar(token, tar_url, sha, install_path, 
                                     tmp_dir, tmp_download_file):
        """
    Downloads and extracts a tar file from a GitHub URL.

    Parameters:
        token (str): The GitHub authentication token.
        tar_url (str): The URL of the tar file to download.
        sha (str): The commit SHA to match the extracted files.
        install_path (str): The path where the files will be installed.
        tmp_dir (str): The temporary directory for extraction.
        tmp_download_file (str): The temporary file path for the downloaded tar file.
    """
        download_tar_from_github(tar_url, token, tmp_download_file)
        create_dir_if_not_exists(install_path)
        remove_and_recreate_dir(tmp_dir)
        with tarfile.open(tmp_download_file, 'r') as tar:
            tar.extractall(tmp_dir)
        untar_dir = [x for x in os.listdir(tmp_dir) if sha in x]
        untar_dir = untar_dir[0]
        content_directory = os.path.join(tmp_dir, untar_dir)
        new_files = os.listdir(content_directory)
        for file_name in new_files:
            shutil.move(os.path.join(content_directory, file_name), os.path.join(install_path, file_name))
        shutil.rmtree(tmp_dir)

def get_random_string(length):
    """
    Generates a random string of lowercase letters of the specified length.

    Parameters:
        length (int): The length of the random string to generate.

    Returns:
        str: A random string of the specified length.
    """
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    logger.debug(f"Random string of length {length} is: {result_str}")
    return result_str